(ns knossos.model.memo
  "We can spend a lot of time comparing models for equality, and
  constructing/GCing models. However, Jepsen tests generally have a relatively
  small state space. A register, for instance, might take on 5 integer values,
  and support 5 reads, 5 writes, and 5*5 compare-and-set transitions.

  So let's do something sneaky. We can read over the history and compute an
  upper bound on its state space. If the space is sufficiently dense, we can
  simply *precompute* the entire state space and transitions and store them as
  a graph. That means:

  - We don't have to actually *run* the model for every transition--we can
  just walk the graph.
  - We can re-use a single copy of each state instead of a new one: fewer
  allocations!
  - Re-using the same arguments and states means we can use referential
  equality comparisons instead of =.
  - We can do truly evil things: because we've enumerated all reachable states,
  and all transitions, we can simply *number* them and represent their
  transitions as an array. A *small* array. We're talking a handful of cache
  lines, and it's immutable. :D

  This computation might be expensive, but we only have to do it once, and it
  can pay by speeding up the exponentially-complex search later.

  We provide a Wrapper type which has the same state space structure as the
  underlying model, but much faster execution. You can perform your search
  using the Wrapper in place of the original Model, then call (model wrapper)
  to obtain the corresponding original Model for that state."
  (:require [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as s]
            [clojure.tools.logging :refer [info]]
            [knossos.op :as op]
            [knossos.model :as model]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import [knossos.model Model]))

(definterface+ Wrapper
  (model [this] "Returns the underlying Model."))

(defn unwrap
  "If x is a Wrapper, return its underlying model. Otherwise, returns x."
  [x]
  (if (instance? Wrapper x)
    (model x)
    x))

(defn canonical-history
  "Returns a copy of history where all equal :fs and :values are replaced by
  identical objects."
  ([history]
   (canonical-history history [] #{} #{}))
  ([history history' fs values]
   (if-not (seq history)
     ; Done
     history'
     (let [op       (first history)
           f        (:f      op)
           value    (:value  op)
           f'       (get fs     f     f)
           value'   (get values value value)
           fs'      (if (identical? f     f')     (conj fs     f)     fs)
           values'  (if (identical? value value') (conj values value) values)]
       (recur (next history)
              (conj history' (assoc op :f f' :value value'))
              fs'
              values')))))

(defn op->transition
  "Maps an operation to a state transition: just its :f and :value."
  [op]
  (select-keys op [:f :value]))

(defn transitions
  "A set of all unique {:f f :value value} transitions for invocations in a
  history."
  [history]
  (->> history
       (filter op/invoke?)
       (map op->transition)
       distinct))

(defn expand-models
  "Given a coll of transitions and a set of models, returns a lazy sequence of
  every model reachable by applying any transition to any model."
  [transitions models]
  (mapcat (fn expand [model] (map (partial model/step model) transitions))
          models))

(def max-state-space
  "We want to avoid burning too much time, or memory, on this memoization
  process. If we think a history may have more than this many reachable states,
  we don't bother memoizing."
  1024)

(defn models
  "Given a coll of transitions and an initial model, computes the complete set
  of all reachable models."
  [transitions model]
  ; We keep track of the previous and next set of models so we can terminate
  ; when the search fails to expand the model set.
  (loop [models   #{}
         models'  (conj models model)]
    (if (= models models')
      ; No new reachable states
      models'
      ; Take the models set
      (->> models'
           ; Compute every reachable model from here
           (expand-models transitions)
           ; Add those to our set of models unless it gets too big
           (reduce (fn [models new-model]
                     (let [models (conj! models new-model)]
                       (when (< max-state-space (count models))
                         ; Too many models; abort
                         (throw+ {:type ::state-space-too-large}))
                       models))
                   (transient models'))
           persistent!
           ; And try again
           (recur models')))))

; model:                the underlying model.
; transition-index:     an integer array mapping operation indices to
;                       transition indices.
; successors:           an array of transition indices to wrappers.
(deftype AWrapper [model
                   ^long hashCode
                   ^ints transition-index
                   ^objects successors]
  Wrapper
  (model [this] model)

  Model
  (step [this op]
    (->> (aget transition-index (int (:index op)))
         (aget successors)))

  Object
  (toString [this]
    (str "(AWrapper " (pr-str model) ")"))

  ; at this time, the reader is encouraged to envision
  ; https://www.youtube.com/watch?v=ec4nMM9KSKA#t=15s
  (equals [this other]
    (identical? this other))

  ; and likewise here
  (hashCode [this]
    hashCode))

(defmethod print-method AWrapper [x ^java.io.Writer w]
  (.write w (str x)))

(defn transition-index
  "Given a history and an array of transitions, computes an int array mapping
  each invocation's :index to the index of the corresponding transition."
  [history transitions]
  (let [transition->i (->> transitions
                           (map-indexed (fn [i x] [x i]))
                           (into {}))
        table         (int-array (count history))]
    (doseq [op history]
      (let [i (-> op op->transition transition->i (or -1))]
        (aset-int table (int (:index op)) i)))
    table))

(defn build-wrappers
  "Computes a map of models to wrappers around those models. Does not link
  wrappers in a succession graph; use link-wrappers! to build the graph."
  [history models transitions]
  (let [transition-index (transition-index history transitions)]
    (->> (for [model models]
           (AWrapper. model
                      (rand-int Integer/MAX_VALUE)
                      transition-index
                      (object-array (count transitions))))
         (zipmap models))))

(defn link-wrappers!
  "Fills in the successors graph for a set of wrappers. Takes a map of models
  to wrappers and an array of transitions. Mutates wrappers in place."
  [models->wrappers transitions]
  ; For each model/wrapper pair, and each transition...
  (doseq [[model ^AWrapper wrapper] models->wrappers
          i               (range (count transitions))]
    ; Compute the resulting model
    (let [model'   (model/step model (nth transitions i))
          wrapper' (if (model/inconsistent? model')
                     model'
                     (models->wrappers model'))]
      ; And update our graph
      (aset ^objects (.successors wrapper) i wrapper')))
  models->wrappers)

(defn wrapper
  "Given a model and a history, returns a Wrapper for that model over that
  history."
  [model history]
  (let [transitions (object-array (transitions history))
        models      (models transitions model)
        wrappers    (build-wrappers history models transitions)]
    (link-wrappers! wrappers transitions)
    (get wrappers model)))

(defn memo
  "Given an initial model and a history, explores the state space exhaustively.
  Returns a map with:

  :history  a version of the history compatible with the returned model.
  :wrapper  a Model and Wrapper which can be used in place of the original
            model. Only compatible with the history provided.

  Guarantees that the wrapper linearizes equivalently to the given model, and
  that (unwrap wrapper) will return the equivalent model at any point in the
  search.

  If the state space is too large to memoize, the returned model will be the
  original model m. (unwrap m) will return m in this case."
  [model history]
  (let [history (canonical-history history)]
    {:history history
     :model   (try+ (wrapper model history)
                   (catch [:type ::state-space-too-large] _
                     (info "More than" max-state-space "reachable models; not memoizing models for this search")
                     model))}))
