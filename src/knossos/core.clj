(ns knossos.core
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [interval-metrics.core :as metrics]
            [potemkin :refer [definterface+]]
            [knossos.prioqueue :as prioqueue]
            [knossos.util :as util]
            [clojure.pprint :refer [pprint]])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.util.concurrent.atomic AtomicLong
                                        AtomicBoolean)))

(defn foldset
  "Folds a reducible collection into a set."
  [coll]
  (r/fold (r/monoid set/union hash-set)
          conj
          coll))

(defn op
  "Constructs a new operation for a history."
  [process type f value]
  {:process process
   :type    type
   :f       f
   :value   value})

(defn invoke-op
  [process f value]
  (op process :invoke f value))

(defn ok-op
  [process f value]
  (op process :ok f value))

(defn fail-op
  [process f value]
  (op process :fail f value))

(defn invoke? [op] (= :invoke (:type op)))
(defn ok?     [op] (= :ok     (:type op)))
(defn fail?   [op] (= :fail   (:type op)))

(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))

(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model."))

(defrecord Inconsistent [msg]
  Model
  (step [this op] this))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

; A read-write register
(defrecord Register [value]
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

(defn processes
  "What processes are in a history?"
  [history]
  (->> history
       (r/map :process)
       (into #{})))

(defn pairs
  "Yields a lazy sequence of [info] | [invoke, ok|fail] pairs from a history]"
  ([history]
   (pairs {} history))
  ([invocations [op & ops]]
   (lazy-seq
     (when op
       (case (:type op)
         :info        (cons [op] (pairs invocations ops))
         :invoke      (do (assert (not (contains? invocations (:process op))))
                          (pairs (assoc invocations (:process op) op) ops))
         (:ok :fail)  (do (assert (contains? invocations (:process op)))
                          (cons [(get invocations (:process op)) op]
                                (pairs (dissoc invocations (:process op))
                                       ops))))))))

(defn remove-failures
  "Returns a version of a history in which none of the operations which are
  known to have failed ever took place at all."
  [history]
  :todo)

(defn complete
  "When a request is initiated, we may not know what the result will be--but
  find out when it completes. In the history, this might look like

  [{:type :invoke
    :f    :read
    :value nil}    ; We don't know what we're going to read.
   {:type  :ok
    :f     :read
    :value 2}]     ; We received 2.

  This function fills in missing values for invocations, where those requests
  complete. It constructs a new history in which we 'already knew' what the
  results of successful operations would have been.

  For failed operations, complete fills in the value for both invocation
  and completion; depending on whichever has a value available."
  [history]
  (->> history
       (reduce
         (fn complete [[history index] op]
           ; History is our complete history of operations. Index is a map of
           ; processes to the index of their most recent invocation. Note that
           ; we assume processes are singlethreaded; e.g. they do not perform
           ; multiple invocations without receiving responses.
           (condp = (:type op)
             ; An invocation; remember where it is
             :invoke
             (let [i (count history)]
               ; Enforce the singlethreaded constraint.
               (when-let [prior (get index (:process op))]
                 (throw (RuntimeException.
                          (str "Process " (:process op) " already running "
                               (pr-str (get history prior))
                               ", yet attempted to invoke "
                               (pr-str op) " concurrently"))))

               [(conj! history op)
                (assoc! index (:process op) i)])

             ; A completion; fill in the completed value.
             :ok
             (let [i           (get index (:process op))
                   _           (assert i)
                   invocation  (nth history i)
                   value       (or (:value invocation) (:value op))]
               [(-> history
                    (assoc! i (assoc invocation :value value))
                    (conj!  op))
                (dissoc! index (:process op))])

             ; A failure; fill in either value.
             :fail
             (let [i           (get index (:process op))
                   _           (assert i)
                   invocation  (nth history i)
                   value       (or (:value invocation) (:value op))]
               [(-> history
                    (assoc! i (assoc invocation :value value))
                    (conj!    (assoc op :value value)))
                (dissoc! index (:process op))])

             ; No change for info messages
             :info
             [(conj! history op) index]))
         [(transient []) (transient {})])
       first
       persistent!))

(defn world
  "A world represents the state of the system at one particular point in time.
  It comprises a known timeline of operations, and a set of operations which
  are pending. Finally, there's an integer index which indicates the number of
  operations this world has consumed from the history."
  [model]
  {:model   model
   :fixed   []
   :pending #{}
   :index   0})

(defn inconsistent-world?
  "Is the model for this world in an inconsistent state?"
  [world]
  (instance? Inconsistent (:model world)))

(defn advance-world
  "Given a world and a series of operations, applies those operations to the
  given world. The returned world will have a new model reflecting its state
  with the given operations applied, and its fixed history will have the given
  history appended. Those ops will also be absent from its pending operations."
  [world ops]
; (prn "advancing" world "with" ops)
  (-> world
      transient
      (assoc! :fixed   (concat (:fixed world) ops))
      (assoc! :model   (reduce step (:model world) ops))
      (assoc! :pending (persistent!
                         (reduce disj! (transient (:pending world)) ops)))
      persistent!))

(defn possible-worlds
  "Given a world, generates all possible future worlds consistent with the
  given world's pending operations. For instance, in the world

  {:fixed [:a :b]
   :pending [:c :d]}

  We *know* :a and :b have already happened, but :c and :d *could* happen, in
  either order, so long as they occur after :a and :b. Here are the possible
  worlds:

  None of them may have happened:

  {:fixed [:a :b]
   :pending [:c :d]}

  One of the two could have happened:

  {:fixed [:a :b :c]
   :pending [:d]}
  {:fixed [:a :b :d]
   :pending [:c]}

  Both could have happened:

  {:fixed [:a :b :c :d]
   :pending []}
  {:fixed [:a :b :d :c]
   :pending []}

  So: we are looking for the permutations of all subsets of all pending
  operations."
  [world]
  (let [worlds (->> world
                    :pending
                    combo/subsets                 ; oh no
                    (r/mapcat combo/permutations) ; dear lord no
                    ; For each permutation, advance the world with those
                    ; operations in order
                    (r/map (fn advance [ops] (advance-world world ops)))
                    ; Filter out null worlds
                    (r/remove nil?)
                    ; Realize
                    r/foldcat)

        ; Filter out inconsistent worlds
        consistent (->> worlds
                        (r/remove inconsistent-world?)
                        r/foldcat)]
    (cond
      ; No worlds at all
      (empty? worlds) worlds

      ; All worlds were inconsistent
      (empty? consistent) (throw (RuntimeException.
                                   (:msg (:model (first worlds)))))

      ; Return consistent worlds
      true consistent)))

(defn fold-invocation-into-world
  "Given a world and a new invoke operation, adds the operation to the pending
  set for the world, and yields a collection of all possible worlds from that.
  Increments world index."
  [world invocation]
  (possible-worlds
    (assoc world
           :index   (inc  (:index world))
           :pending (conj (:pending world) invocation))))

(defn fold-invocation-into-worlds
  "Given a sequence of worlds and a new invoke operation, adds this operation
  to the pending set for each, and projects out all possible worlds from
  those."
  [worlds invocation]
  (r/mapcat #(fold-invocation-into-worlds % invocation) worlds))

(defn fold-completion-into-world
  "Given a world and a completion operation, returns world if the operation
  took place in that world, else nil. Advances the world index by one."
  [world completion]
  (let [p (:process completion)]
    (when-not (some #(= p (:process %)) (:pending world))
      (assoc world :index (inc (:index world))))))

(defn fold-completion-into-worlds
  "Given a sequence of worlds and a completion operation, returns only those
  worlds where that operation took place; e.g. is not still pending.

  TODO: replace the corresponding element in the history with the completion."
  [worlds completion]
  (->> worlds
       (r/map #(fold-completion-into-world % completion))
       (r/remove nil)))

(defn fold-failure-into-world
  "Given a world and a failed operation, returns world if the operation did
  *not* take place, and removes the operation from the pending ops in that
  world. Advances world index by one.

  Note that a failed operation is an operation which is *known* to have failed;
  e.g. the system *guarantees* that it did not take place. This is different
  from an *indeterminate* failure."
  [world failure]
  (let [process (:process failure)
        pending (:pending world)]
    ; Find the corresponding invocation
    (when-let [inv (some #(when (= process (:process %)) %) pending)]
      ; In this world, we have not yet applied the operation.
      (assoc world :index   (inc (:index world))
                   :pending (disj pending inv)))))

(defn fold-failure-into-worlds
  "Given a sequence of worlds and a failed operation, returns only those worlds
  where that operation did not take place, and removes the operation from
  the pending ops in those worlds."
  [worlds failure]
  (->> worlds
       (r/map #(fold-failure-into-world % failure))
       (r/remove nil?)))

(defn maybe-list
  "If x is nil, returns the empty list. If x is not-nil, returns (x)."
  [x]
  (if x (list x) '()))

(defn fold-op-into-world
  "Given a world and any type of operation, folds that operation into the world
  and returns a sequence of possible worlds. Increments the world index."
  [world op]
  (condp = (:type op)
    :invoke (fold-invocation-into-world world op)
    :ok     (maybe-list (fold-completion-into-world world op))
    :fail   (maybe-list (fold-failure-into-world    world op))
    :info   (list (assoc world :index (inc (:index world))))))

(defn fold-op-into-worlds
  "Given a set of worlds and any type of operation, folds that operation into
  the set and returns a new set of possible worlds."
  [worlds op]
  (->> worlds
       (r/mapcat #(fold-op-into-world % op))
       foldset))

(defn linearizations
  "Given a model and a history, returns all possible worlds where that history
  is linearizable."
  [model history]
  (reduce fold-op-into-worlds
          (list (world model))
          history))

(defn degenerate-worlds
  "NEXT LEVEL ALGORITHMIC MANEUVER: if all we care about is whether a history
  is linearizable *at all*, instead of exactly how it got there, we can pull A
  SNEAKY TRICK by only choosing worlds which *could* linearize differently in
  the future.

  In particular, we exploit the fact that the past is a fiction;
  how we got to a given model state has no impact on the model's future
  evolution. If two models have identical models and pending sets we may ignore
  their history.

  Takes a set of worlds and returns an equivalent set of worlds."
  [worlds]
  (->> worlds
       (r/map (fn index [world] {[(:model world) (:pending world)]
                                 world}))
       (r/fold merge)
       (r/map (fn [_ v] v))
       foldset))

(defn linearizable-prefix-and-worlds
  "Returns a vector consisting of the linearizable prefix and the worlds
  just prior to exhaustion."
  [model history]
  (reduce (fn [[linearizable worlds] op]
            (let [worlds'  (fold-op-into-worlds worlds op)
                  worlds'' (degenerate-worlds worlds')]
              (prn :world-size (count worlds')
                   :degenerate (count worlds''))
              (if (empty? worlds'')
                ; Out of options
                (reduced [linearizable worlds])
                [(conj linearizable op) worlds''])))
          [[] (list (world model))]
          history))

(def awfulness-comparator
  "Which one of these worlds should we explore first?"
  (reify java.util.Comparator
    (compare [this a b]
      (cond (< (count (:pending a)) (count (:pending b))) -1
            (< (:index a)           (:index b))            1
            :else                                          0))))

(defn degenerate-world-key
  "An object which uniquely identifies whether or not a world is linearizable.
  If two worlds have the same degenerate-world-key (in the context of a
  history), their linearizability is equivalent."
  [world]
  ; What kind of social studies IS this?
  (dissoc world :history))

(defn seen-world!?
  "Given a mutable hashmap of seen worlds, ensures that an entry exists for the
  given world, and returns truthy iff that world had already been seen."
  [^NonBlockingHashMapLong seen world]
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
        (when (< 1 (count (:pending world)))
          (.put seen h k))
        false))))

(defn short-circuit!
  "If we've reached a world with an index as deep as the history, we can
  abort all threads immediately."
  [history ^AtomicBoolean running? world]
  (when (= (count history) (:index world))
;    (info "Short-circuiting" world)
    (.set running? false)))

(defn update-deepest-world!
  "If this is the deepest world we've seen, add it to the deepest list."
  [deepest world]
  (when (<= (:index (first @deepest)) (:index world))
    (swap! deepest (fn update [deepest]
                     (let [index  (:index (first deepest))
                           index' (:index world)]
                       (cond (< index index') [world]
                             (= index index') (conj deepest world)
                             :else            deepest))))))

(defn explore-world!
  "Explores a world's direct successors, reinjecting each into `leaders`.
  Returns the number of worlds reinserted into leaders. Uses the `seen` cache
  to avoid exploring worlds already visited. Updates `deepest` with new worlds
  at the highest index in the history. Guarantees that by return time, all
  worlds reinjected into leaders will be known to be extant."
  [history ^AtomicBoolean running? leaders seen deepest stats world]
  (when (< (:index world) (count history))
    (let [op         (nth history (:index world))
;          _          (info "op" op "world\n"
;                           (with-out-str (pprint world)))
          worlds     (fold-op-into-world world op)
          reinserted (reduce
                       (fn reinjector [reinserted world]
                         ; Jacques-Yves Cousteau could be thrilled
                         (update-deepest-world! deepest world)

                         ; Done?
                         (short-circuit! history running? world)

                         (if (seen-world!? seen world)
                           ; Definitely been here before
                           (do (metrics/update! (:skipped-worlds stats) 1)
 ;                              (info "Skipping\n" (with-out-str (pprint world)))
                               reinserted)

                           ; O brave new world, that hath such operations in it!
                           (do (metrics/update! (:visited-worlds stats) 1)
;                               (info "reinjecting\n" (with-out-str (pprint world)))
                               (.incrementAndGet
                                 ^AtomicLong (:extant-worlds stats))

                               (prioqueue/put! leaders world)
                               (inc reinserted))))
                       0
                       worlds)]
      reinserted)))

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
  nearby worlds just slightly ahead of ours, using fold-op-into-world. If there
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
  (if (empty? history)
    [history (world model)]
    (let [world    (world model)
          threads  48
          leaders  (prioqueue/prioqueue awfulness-comparator)
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
                                 (.get (:extant-worlds stats)) "extant worlds,"
                                 (long visited) "visited/s,"
                                 (long skipped) "skipped/s,"
                                 "hitrate" (format "%.3f" hitrate)
                                 "cache size" (.size seen))))))]

      ; Start with a single world containing the initial state
      (prioqueue/put! leaders world)

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

(defn analysis
  "Returns a map of information about the linearizability of a history.
  Completes the history and searches for a linearization."
  [model history]
  (let [history+            (complete history)
        [lin-prefix worlds] (linearizable-prefix-and-worlds model history+)
        valid?              (= (count history+) (count lin-prefix))
        evil-op             (when-not valid?
                              (nth history+ (count lin-prefix)))]
    (if valid?
      {:valid?              true
       :linearizable-prefix lin-prefix
       :worlds              worlds}
      {:valid?                   false
       :linearizable-prefix      lin-prefix
       :last-consistent-worlds   worlds
       :inconsistent-op          evil-op
       :inconsistent-transitions (map (fn [w]
                                      [(:model w)
                                       (-> w :model (step evil-op) :msg)])
                                      worlds)})))
