(ns knossos.history
  "Operations on histories"
  (:require [clojure.core.reducers :as r]
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

(defn processes
  "What processes are in a history?"
  [history]
  (->> history
       (r/map :process)
       (into #{})))

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
         :invoke      (do (assert (not (contains? invocations (:process op))))
                          (pairs (assoc invocations (:process op) op) ops))
         (:ok :fail)  (do (assert (contains? invocations (:process op)))
                          (cons [(get invocations (:process op)) op]
                                (pairs (dissoc invocations (:process op))
                                       ops))))))))

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

    ; A failure; fill in either value.
    :fail
    (let [i           (get index (:process op))
          _           (assert i)
          invocation  (nth history i)
          value       (or (:value invocation) (:value op))
          invocation' (assoc invocation :value value, :fails? true)]
      [(-> history
           (assoc! i invocation')
           (conj!    (assoc op :value value)))
       (dissoc! index (:process op))])

    ; No change for info messages
    :info
    [(conj! history op) index]))

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
  and completion; depending on whichever has a value available. We *also* add a
  :fails? key to invocations which will fail, allowing checkers to skip them."
  [history]
  ; Reducing with a transient means we just have "vector", not "vector of ops"
  (->> history
       (reduce complete-fold-op
               [(transient []) (transient {})])
       first
       persistent!))

(defn index
  "Attaches an :index key to each element of the history, identifying its
  position in the history vector."
  [history]
  (->> history
       (mapv (fn [i op] (assoc op :index i)) (range))
       vec))
