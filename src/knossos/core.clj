(ns knossos.core
  (:refer-clojure :exclude [defn fn])
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.core.typed :as t
                                :refer [
                                        All
                                        Any
                                        ann
                                        ann-form
                                        ann-record
                                        Atom1
                                        Atom2
                                        defalias
                                        defn
                                        fn
                                        HMap
                                        HVec
                                        I
                                        IFn
                                        inst
                                        NonEmptyVec
                                        Option
                                        Set
                                        Seqable
                                        tc-ignore
                                        U
                                        Vec
                                        ]]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [interval-metrics.core :as metrics]
            [potemkin :refer [definterface+
                              defrecord+]]
            [knossos.prioqueue :as prioqueue]
            [knossos.util :as util]
            [knossos.op :as op :refer [Op Invoke OK Info Fail]]
            [knossos.history :as history]
            [knossos.extra-types]
            [clojure.pprint :refer [pprint]])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.util.concurrent.atomic AtomicLong
                                        AtomicBoolean)
           (clojure.tools.logging.impl Logger
                                       LoggerFactory)
           (interval_metrics.core Metric)))

(def op op/op)

(def invoke-op  op/invoke)
(def ok-op      op/ok)
(def fail-op    op/fail)

(def invoke? op/invoke?)
(def ok?     op/ok?)
(def fail?   op/fail?)

(ann ^:no-check step [Model Op -> Model])

(tc-ignore ; core.typed can't typecheck interfaces

(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model."))

(defrecord+ Inconsistent [msg]
  Model
  (step [this op] this))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

) ; tc-ignore

(ann  inconsistent? [Model -> Boolean])
                     ; core.typed: Can't use these filters without getting a
                     ; ClassNotFound exception for Model. Why???
                     ; :filters {:then (is Inconsistent 0)
                     ; :else (!  Inconsistent 0)}])
(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

(tc-ignore ; without interface types, we can't type implementations

(defrecord+ NoOp []
  Model
  (step [m op] m))

(def noop
  "A model which always returns itself, unchanged."
  (NoOp.))

(defrecord+ Register [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (Register. (:value op))
      :read  (if (or (nil? (:value op))     ; We don't know what the read was
                     (= value (:value op))) ; Read was a specific value
               r
               (inconsistent
                 (str "read " (pr-str (:value op))
                      " from register " value))))))

(defn register
  "A read-write register."
  ([] (Register. nil))
  ([x] (Register. x)))

(defrecord+ CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str "can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str "can't read " (:value op)
                                  " from register " value))))))

(defn cas-register
  "A compare-and-set register"
  ([]      (CASRegister. nil))
  ([value] (CASRegister. value)))

(defrecord+ Mutex [locked?]
  Model
  (step [r op]
    (condp = (:f op)
      :acquire (if locked?
                 (inconsistent "already held")
                 (Mutex. true))
      :release (if locked?
                 (Mutex. false)
                 (inconsistent "not held")))))

(defn mutex
  "A single mutex responding to :acquire and :release messages"
  []
  (Mutex. false))

) ; OK back to typechecking

(ann-record World [model   :- Model
                   fixed   :- (Vec Op)
                   pending :- (Set Op)
                   index   :- Long])
(defrecord+ World [model fixed pending index])

(defn world
  "A world represents the state of the system at one particular point in time.
  It comprises a known timeline of operations, and a set of operations which
  are pending. Finally, there's an integer index which indicates the number of
  operations this world has consumed from the history."
  [model :- Model] :- World
  (World. model [] #{} 0))

(ann-record DegenerateWorld [model   :- Model
                             pending :- (Set Op)
                             index   :- Long])
(defrecord+ DegenerateWorld [model pending index])

(defn degenerate-world-key
  "An object which uniquely identifies whether or not a world is linearizable.
  If two worlds have the same degenerate-world-key (in the context of a
  history), their linearizability is equivalent.

  Here we take advantage of the fact that worlds with equivalent pending
  operations and equivalent positions in the history will linearize
  equivalently regardless of exactly what fixed path we took to get to this
  point. This degeneracy allows us to dramatically prune the search space."
  [^World world :- World] :- DegenerateWorld
  (DegenerateWorld. (.model world)
                    (.pending world)
                    (.index world)))

(defn inconsistent-world?
  "Is the model for this world in an inconsistent state?"
  [world :- World] :- Boolean
  (instance? Inconsistent (:model world)))

(ann ^:no-check outof
     (All [a] [(Set a) (Seqable Any) -> (Set a)]))
(defn outof
  "Like into, but uses `disj` on a hashset"
  [coll things-to-remove]
  (->> things-to-remove
       (reduce disj! (transient coll))
       persistent!))

(defn advance-world
  "Given a world and a series of operations, applies those operations to the
  given world. The returned world will have a new model reflecting its state
  with the given operations applied, and its fixed history will have the given
  history appended. Those ops will also be absent from its pending operations.
  Does not affect the world index."
  [^World world :- World, ops :- (Seqable Op)] :- World
; (prn "advancing" world "with" ops)
  (World. (reduce step (.model world) ops)
          (into (.fixed world) ops)
          (outof (.pending world) ops)
          (.index world)))

(defn fold-invocation-into-world
  "Given a world and a new invoke operation, adds the operation to the pending
  set for the world, and increments the world's index."
  [world :- World, invocation :- Invoke] :- World
;  (info "Fold" invocation "into" world)
  (assoc world
         :index   (inc  (:index world))
         :pending (conj (:pending world) invocation)))

(defalias Deepest
  "Mutable set of deepest worlds"
  (Atom1 (Vec World)))

(defn update-deepest-world!
  "If this is the deepest world we've seen, add it to the deepest list. Returns
  world."
  [deepest :- Deepest, world :- World] :- World
  (when (<= (or (:index (first @deepest) -1) (:index world)))
    (swap! deepest (fn update [deepest :- (Vec World)] :- (Vec World)
                     (let [index  (or (:index (first deepest)) -1)
                           index' (:index world)]
                       (ann-form index Long)
                       (ann-form index' Long)
                       (cond (< index index') [world]
                             (= index index') (conj deepest world)
                             :else            deepest)))))
  world)

(ann ^:no-check pull-out
     (All [a] [[a -> Any] (Seqable a) -> (HVec [(Option a) (Vec a)])]))
(defn pull-out
  "Given a predicate function and a collection, returns a pair of `[matched,
  others]`, where `matched` is the first element matched by the predicate fn,
  and others is a vector of all other elements."
  [pred coll]
  (loop [match    nil
         others   (transient [])
         coll     coll]
    (if-let [s (seq coll)]
      (let [x  (first s)
            xs (rest s)]
        (if (pred x)
          (if (nil? match)
            (recur x others xs)
            (throw
              (IllegalStateException.
                (str "Multiple matches for predicate: already had " match))))
          (recur match (conj! others x) xs)))
      [match (persistent! others)])))

(ann ^:no-check fold-completion-into-world* [World OK -> (Seqable World)])
(defn fold-completion-into-world*
  "Given a world and a completion operation, returns all possible worlds where
  that operation took place--including inconsistent worlds. Increments the
  world index by one."
  [world :- World, completion :- OK] :- (Seqable World)
;  (info "Fold" completion "into" world)
  (let [world      (assoc world :index (inc (:index world)))
        p          (:process completion)
        finder     (fn finder [op :- Op] :- (Option Op)
                     (when (= p (:process op)) op))
        ; Find the invocation from this world's pending set, and build up a
        ; vector of *other* invocations
        [invocation pending'] (pull-out finder (:pending world))]

    (cond
      ; We have a pending invocation. Take all operations from *other* threads
      ; I *cannot* convince core.typed that invocation is non-nil here, so this
      ; is uncheckable presently.
      invocation
      (->> pending'

           ; Compute all possible partial orders of those operations
           combo/subsets
           (r/mapcat (inst combo/permutations Op))

           (r/map (fn advance [ops :- (Vec Op)] :- World
                    ; Apply the potential pending ops, then this invocation.
                    (-> world
                        (advance-world ops)
                        (advance-world (list invocation))))))

      ; What if it already happened in this timeline?
      (some finder (rseq (:fixed world)))
      (list world)

      ; We never saw any invocation
      true
      (throw (ex-info "Couldn't find an invocation for completion"
                      {:completion completion
                       :world world})))))

(defn fold-completion-into-world
  "Given a world, and a completion operation, returns all possible worlds
  consistent with that operation taking place. Increments world index by one.
  This technique derives from the symmetry reduction in Lowe 2015, Testing For
  Linearizability."
  [world :- World, completion :- OK] :- (Seqable World)
  (->> (fold-completion-into-world* world completion)
       (r/remove inconsistent-world?)))

(defn fold-failure-into-world
  "Given a world and a failed operation, returns world, but with the world's
  index advanced by one. Failures are informational only; they do not affect
  linearizability.

  Note that a failed operation is an operation which is *known* to have failed;
  e.g. the system *guarantees* that it did not take place. This is different
  from an *indeterminate* failure, and is also different than an operation
  which is *known to have been impossible*! :-O"
  [world :- World, failure :- Fail] :- (Option World)
;  (info "Fold" failure "into" world)
  (assoc world :index (inc (:index world))))

(defn fold-info-into-world
  "Given a world and an info operation, returns a subsequent world. Info
  operations don't appear in the fixed history; only the index advances."
  [world :- World, info :- Info] :- World
  (assoc world :index (inc (:index world))))

(defn next-op
  "The next operation from the history to be applied to a world, based on the
  world's index, or nil when out of bounds."
  [history :- (Vec Op), world :- World] :- (Option Op)
  (try
    (nth history (:index world))
;    (catch NullPointerException e
;      (info (with-out-str (pprint world)))
;      (throw e))
    (catch IndexOutOfBoundsException e
      nil)))

(defalias Stats
  "Statistics for tracking analyzer performance"
  (HMap :mandatory {:extant-worlds  AtomicLong
                    :skipped-worlds Metric
                    :visited-worlds Metric}))

(defn seen-world!?
  "Given a mutable hashmap of seen worlds, ensures that an entry exists for the
  given world, and returns truthy iff that world had already been seen."
  [^NonBlockingHashMapLong seen :- NonBlockingHashMapLong,
   world :- World] :- Boolean
  (let [k  (degenerate-world-key world)
        ; Constrain the number of possible elements in the cache
        h (bit-and 0xffffff (hash k))
        seen-key (.get seen h)]
    (if (= k seen-key)
      ; We've already visited this node.
      true
      ; Null or collision. Replace the existing value.
      (do
        ; We want to avoid hitting shared state for cheap operations, so we
        ; only write to the cache if this world is sufficiently expensive to
        ; visit.
        (when (< 0 (count (:pending world)))
          (.put seen h k))
        false))))

; Core.typed can't infer that the (:type op) check constrains Ops to their
; subtypes like Invoke, OK, etc.
(defn prune-world
  "Given a history and a world, advances the world through as many operations
  in the history as possible, without splitting into multiple worlds. Returns a
  new world (possibly the same as the input), or nil if the world was found to
  be inconsistent."
  [history :- (Vec Op)
   seen    :- NonBlockingHashMapLong
   deepest :- Deepest
   stats   :- Stats
   world   :- (Option World)]
  (when world
    ; Strictly speaking we do a little more work than necessary by having
    ; this here, but atomic reads are pretty cheap and contention should be
    ; infrequent.
    ; Jacques-Yves Cousteau could be thrilled
    (update-deepest-world! deepest world)

    (if (seen-world!? seen world)
      ; Definitely been here before
      (do (metrics/update! (:skipped-worlds stats) 1)
          ; (info "Skipping\n" (with-out-str (pprint world)))
          nil)

      (do ; OK, we haven't seen this world before.
          (metrics/update! (:visited-worlds stats) 1)

          (let [op (next-op history world)]
            (if (or (nil? op) (ok? op))
              ; We hit a bifurcation point or the end
              world
              (recur history seen deepest stats
                     (cond (op/invoke? op) (fold-invocation-into-world world op)
                           (op/fail? op) (fold-failure-into-world world op)
                           (op/info? op) (fold-info-into-world world op)))))))))

; core.typed can't infer that (I (U a b) (Not b)) is a, which prevents us from
; calling (r/remove nil?) and knowing the result is a seq of worlds.
;
; user=> (cf (fn [x :- (Seqable (Option Long))] (remove nil? x)))
; [[(Seqable (Option java.lang.Long))
;   -> (ASeq (I (U nil Long) (Not nil)))] {:then tt, :else ff}]
(ann ^:no-check explode-then-prune-world
     [(Vec Op) NonBlockingHashMapLong Deepest Stats World -> (Seqable World)])
(defn explode-then-prune-world
  "Given a history and a world, generates a reducible sequence of possible
  subseqeuent worlds, obtained in two phases:

  1. If the next operation in the history for this world is an ok op, we
     find all possible subsequent worlds as a result of that completion.

  2. For all immediately subsequent invoke, fail, and info operations, use
     those operations to prune and further advance the worlds from phase 1."
  [history seen deepest stats world]
  (if-let [op (next-op history world)]
    ; Branch out for all completions
    (->> (if (op/ok? op)
           (let [ws (fold-completion-into-world world op)]
             ;(info "completion expanded into" (with-out-str
             ;                                   (pprint (into [] ws))))
             ws)
           (list world))
         ; Prune other ops
         (r/map (fn shears [world :- World] :- (Option World)
                  (prune-world history seen deepest stats world)))
         (r/remove nil?))
    ; No more ops
    (list world)))

(defn short-circuit!
  "If we've reached a world with an index as deep as the history, we can
  abort all threads immediately."
  [history                  :- (Vec Op)
   ^AtomicBoolean running?  :- AtomicBoolean
   world                    :- World] :- Any
  (when (= (count history) (:index world))
;    (info "Short-circuiting" world)
    (.set running? false)))

(defn ^Long awfulness
  "How bad is this world to explore?"
  [world :- World] :- Long
  (long (- (:index world))))

(tc-ignore ; No idea how to type leaders, giving up

(defn explore-world!
  "Explores a world's direct successors, reinjecting each into `leaders`.
  Returns the number of worlds reinserted into leaders. Uses the `seen` cache
  to avoid exploring worlds already visited. Updates `deepest` with new worlds
  at the highest index in the history. Guarantees that by return time, all
  worlds reinjected into leaders will be known to be extant."
  [history ^AtomicBoolean running? leaders seen deepest stats world]
  (->> world
       (explode-then-prune-world history seen deepest stats)
       (reduce
         (fn reinjector [reinserted world]
           ; Done?
           (short-circuit! history running? world)

           ; O brave new world, that hath such operations in it!
           (do (.incrementAndGet ^AtomicLong (:extant-worlds stats))
               ; (info "reinjecting\n" (with-out-str (pprint world)))
               (prioqueue/put! leaders (awfulness world) world)
               (inc reinserted)))
         0)))

(defn explorer
  "Pulls worlds off of the leader atom, explores them, and pushes resulting
  worlds back onto the leader atom."
  [history ^AtomicBoolean running? leaders seen deepest stats i]
  (future
    (util/with-thread-name (str "explorer-" i)
      (try
        (while (and (.get running?)
                    (pos? (.get ^AtomicLong (:extant-worlds stats))))
          (when-let [world (prioqueue/poll! leaders 10)]
            ; Explore world, possibly creating new ones
            (explore-world! history running? leaders seen deepest stats world)

            ; We're done with this world now.
            (.decrementAndGet ^AtomicLong (:extant-worlds stats))))

        ; We've exhausted all possible worlds
;        (info "worker" i "exiting")
        (.set running? false)

      (catch Throwable t
        (warn t "explorer" i "crashed!")
        (throw t))))))

(defn linearizable-prefix-and-worlds
  "Returns a vector consisting of the longest linearizable prefix and the
  worlds just prior to exhaustion.

  If you think about a lightning strike, where the history stretches from the
  initial state in the thundercloud to the final state somewhere in the ground,
  we're trying to find a path--any path--for a lightning bolt to jump from
  cloud to ground.

  Given a world at the tip of the lightning bolt, we can reach out to several
  nearby worlds just slightly ahead of ours, using fold-x-into-world. If there
  are no worlds left, we've struck a dead end and that particular fork of the
  lightning bolt terminates. If there *are* worlds left, we want to explore
  them--but it's not clear in what order.

  Moreover, some paths are more expensive to traverse than others. Worlds with
  a high number of pending operations, for instance, are particularly expensive
  because each step explores n! operations. If we can find a *quicker* path to
  ground, we should take it.

  We do this by keeping a set of all incomplete worlds, and following the
  worlds that seem to be doing the best. We leave the *hard* worlds for later.
  Each thread pulls a world off of the incomplete set, explodes it into several
  new worlds, and pushes those worlds back into the set. We call this set
  *leaders*.

  If we reach a world which has no future operations--whose index is equal to
  the length of the history--we've found a linearization and can terminate.

  If we ever run *out* of leaders, then we know no linearization is possible.
  Disproving linearizability can be much more expensive than proving it; we
  have to keep trying and trying until every possible option has been
  exhausted."
  [model history]
  (assert (vector? history))
  (if (empty? history)
    [history [(world model)]]
    (let [world    (world model)
          threads  (+ 2 (.. Runtime getRuntime availableProcessors))
          leaders  (prioqueue/prioqueue)
          seen     (NonBlockingHashMapLong.)
          running? (AtomicBoolean. true)
          stats    {:extant-worlds  (AtomicLong. 1)
                    :skipped-worlds (metrics/rate)
                    :visited-worlds (metrics/rate)}
          deepest (atom [world])
          workers  (->> (range threads)
                        (map (partial explorer history running? leaders
                                      seen deepest stats))
                        doall)
          reporter (future
                     (util/with-thread-name "reporter"
                       (while (.get running?)
                         (Thread/sleep 5000)
                         (let [visited    (metrics/snapshot!
                                            (:visited-worlds stats))
                               skipped    (metrics/snapshot!
                                            (:skipped-worlds stats))
                               total      (+ visited skipped)
                               hitrate    (if (zero? total) 1 (/ skipped total))
                               depth      (:index (first @deepest))
                               depth-frac (/ depth (count history))]
                           (info (str "[" depth " / " (count history) "]")
                                 (.get ^AtomicLong (:extant-worlds stats))
                                 "extant worlds,"
                                 (long visited) "visited/s,"
                                 (long skipped) "skipped/s,"
                                 "hitrate" (format "%.3f," hitrate)
                                 "cache size" (.size seen))))))]

      ; Start with a single world containing the initial state
      (prioqueue/put! leaders 0 world)

      ; Wait for workers
      (->> workers (map deref) dorun)
      (future-cancel reporter)

;      (info "Final queue was"
;            (take-while identity
;                        (repeatedly #(prioqueue/poll! leaders 0))))

;      (info "Final stats:" (with-out-str (pprint stats)))

      ; Return prefix and deepest world
      (let [deepest @deepest]
        [(take (:index (first deepest)) history) deepest]))))

(defn linearizable-prefix
  "Computes the longest prefix of a history which is linearizable."
  [model history]
  (first (linearizable-prefix-and-worlds model history)))

(defalias Autopsy
  (HMap :mandatory {}
        :complete? false))

(defn autopsy
  "Given a known good world and an inconsistent world following it,
  reconstructs what happened just prior to its death."
  [history    :- (Vec Op)
   antemortem :- World
   postmortem :- World] :- Autopsy
  (assert (not (inconsistent-world? antemortem)))
  (assert (inconsistent-world? postmortem))
  (assert (vector? history))
  (assert (instance? World antemortem))
  (assert (instance? World postmortem))
  (let [; Where were we in the history at the time of death?
        forcing-op     (nth history (dec (:index postmortem)))
        index          (:index antemortem)
        index'         (:index postmortem)
        _              (assert (< -1 index index' (count history)))

        ; What were the initial and crashed model states?
        model          (:model antemortem)
        model'         (:model postmortem)

        ; Find path that led to this crash
        path           (subvec (:fixed postmortem)
                               (count (:fixed antemortem)))
        _              (assert (= (:fixed postmortem)
                                  (concat (:fixed antemortem)
                                          path)))

        ; Play forward model until just prior to death
        perimortem     (-> (advance-world antemortem (drop-last path))
                           (assoc :index (+ (:index antemortem)
                                            (dec (count path)))))
        _              (assert (not (inconsistent-world? perimortem)))]

    {:world       perimortem
     :op          (peek path)
     :error       (:msg (:model postmortem))}))

(defn inconsistent-transitions
  "Takes a world close to death--one which is one operation away from running
  out of linearizable paths, and returns a sequence of maps showing all the
  ways it failed to be linearizable."
  [history :- (Vec Op), world :- World] :- (Seqable Autopsy)
  (when-let [op (next-op history world)]
    (cond
      (op/ok? op) (->> (fold-completion-into-world* world op)
                       (r/map (partial autopsy history world))
                       (into []))
      :else       {:not-sure op})))

(defn analysis
  "Returns a map of information about the linearizability of a history.
  Completes the history and searches for a linearization."
  [model history]
  (let [history+            (history/complete history)
        [lin-prefix worlds] (linearizable-prefix-and-worlds model history+)
        valid?              (= (count history+) (count lin-prefix))
        evil-op             (when-not valid?
                              (nth history+ (count lin-prefix)))

        ; Remove worlds with equivalent states
        worlds              (->> worlds
                                 ; Wait, is this backwards? Should degenerate-
                                 ; world-key be the key in this map?
                                 (r/map (juxt degenerate-world-key identity))
                                 (into {})
                                 vals)]
    (if valid?
      {:valid?              true
       :linearizable-prefix lin-prefix
       :worlds              worlds}
      {:valid?                   false
       :linearizable-prefix      lin-prefix
       :inconsistent-op          evil-op
       :causes                   (->> worlds
                                      (mapcat (partial inconsistent-transitions
                                                    history)))})))

)
