(ns knossos.history
  "Operations on histories"
  (:require [clojure.core.reducers :as r]
            [clojure.core.typed :refer [ann
                                        ann-form
                                        Any
                                        AnyInteger
                                        All
                                        defalias
                                        HMap
                                        HVec
                                        I
                                        IFn
                                        Map
                                        Option
                                        Seqable
                                        Set
                                        U
                                        Value
                                        Vec]]
            [knossos.extra-types]
            [knossos.op :as op])
  (:import [clojure.core.protocols CollReduce]
           [clojure.lang IMapEntry
                         IPersistentMap
                         IPersistentSet
                         IPersistentVector
                         IPersistentCollection
                         ITransientMap
                         ITransientSet
                         ITransientVector
                         ITransientCollection]))

(defalias History
  "A history is a sequence of operations."
  (I (Seqable op/Op)
     clojure.lang.Sequential))

(ann ^:no-check processes [History -> (Set Object)])
(defn processes
  "What processes are in a history?"
  [history]
  (->> history
       (r/map :process)
       (into #{})))

(ann ^:no-check pairs (IFn [History
                  -> (Seqable (U (HVec [op/Info])
                                 (HVec [op/Invoke (U op/OK op/Fail)])))]
                 [(Map Object op/Invoke) (Option History)
                  -> (Seqable (U (HVec [op/Info])
                                 (HVec [op/Invoke (U op/OK op/Fail)])))]))
(defn pairs
  "Pairs up ops from each process in a history. Yields a lazy sequence of [info]
  or [invoke, ok|fail] pairs."
  ([history]
   (pairs {} history))
  ([invocations [op & ops]]
   (lazy-seq
     (when op
       (case (:type op)
         :info        (cons [op] (pairs invocations ops))
         :invoke      (do (ann-form op op/Invoke)
                          (assert (not (contains? invocations (:process op))))
                          (pairs (assoc invocations (:process op) op) ops))
         (:ok :fail)  (do (assert (contains? invocations (:process op)))
                          (cons [(get invocations (:process op)) op]
                                (pairs (dissoc invocations (:process op))
                                       ops))))))))

(ann ^:no-check
     complete-fold-op [(HVec [ITransientVector ITransientMap]) op/Op ->
                       (HVec [ITransientVector ITransientMap])])
(defn complete-fold-op
  "Folds an operation into a completed history, keeping track of outstanding
  invocations.

  History is our complete history of operations: a transient vector. Index is a
  transient map of processes to the index of their most recent invocation. Note
  that we assume processes are singlethreaded; e.g. they do not perform
  multiple invocations without receiving responses."
  [[history index] op]
  (condp = (:type op)
    ; An invocation; remember where it is
    :invoke
    (do
      ; Enforce the singlethreaded constraint.
      (when-let [prior (get index (:process op))]
        (throw (RuntimeException.
                 (str "Process " (:process op) " already running "
                      (pr-str (get history prior))
                      ", yet attempted to invoke "
                      (pr-str op) " concurrently"))))

      [(conj! history op)
       (assoc! index (:process op) (dec (count history)))])

    ; A completion; fill in the completed value.
    :ok
    (let [i           (get index (:process op))
          _           (assert i)
          invocation  (nth history i)
          value       (or (:value invocation) (:value op))
          invocation' (assoc invocation :value value)]
      [(-> history
           (assoc! i invocation')
           (conj! op))
       (dissoc! index (:process op))])

    ; A failure; rewrite to an info.
    :fail
    (let [i           (get index (:process op))
          _           (assert i)
          invocation  (nth history i)
          value       (or (:value invocation) (:value op))
          invocation' (assoc invocation
                             :type :info,
                             :value [:failed-invoke value])]
      [(-> history
           (assoc! i invocation')
           (conj!  (assoc op :value value)))
       (dissoc! index (:process op))])

    ; No change for info messages
    :info
    [(conj! history op) index]))

(ann ^:no-check complete [History -> History])
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

  Complete transforms invocation of failed operations into :info ops as well."
  [history]
  ; Reducing with a transient means we just have "vector", not "vector of ops"
  (->> history
       (reduce complete-fold-op
               [(transient []) (transient {})])
       first
       persistent!))
