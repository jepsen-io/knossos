(ns knossos.history
  "Operations on histories"
  (:require [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
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

(defn parse-ops
  "We're going to construct Op defrecords throughout our analysis, which means
  that a map without a value, e.g. {:type :invoke, :f :lock, :process 2} is not
  equal to the corresponding Op, which will have an explicit :value nil. We
  convert all maps in the history to Ops before doing anything else."
  [history]
  (mapv op/map->Op history))

(defn unmatched-invokes
  "Which invoke ops in a history have no corresponding :ok, :fail, or :info?"
  [history]
  (loop [history (seq history)
         calls   (transient {})]
    (if-not history
      (vals (persistent! calls))
      (let [op      (first history)
            process (:process op)]
        (recur (next history)
               (cond (op/invoke? op) (assoc! calls process op)
                     (op/ok? op)     (dissoc! calls process)
                     (op/fail? op)   (dissoc! calls process)
                     (op/info? op)   (dissoc! calls process)
                     true            (throw (IllegalArgumentException.
                                              (str "Unknown op type " (:type op) " from " (pr-str op))))))))))

(defn crashed-invokes
  "Which invoke ops in a history have no corresponding :ok or :fail?"
  [history]
  (loop [history (seq history)
         calls   (transient {})]
    (if-not history
      (vals (persistent! calls))
      (let [op      (first history)
            process (:process op)]
        (recur (next history)
               (cond (op/invoke? op) (assoc! calls process op)
                     (op/ok? op)     (dissoc! calls process)
                     (op/fail? op)   (dissoc! calls process)
                     (op/info? op)   calls))))))

(defn processes
  "What processes are in a history?"
  [history]
  (->> history
       (r/map :process)
       (into #{})))

(defn sort-processes
  "Sort a collection of processes. Puts numbers second, keywords first."
  [coll]
  (sort (fn comparator [a b]
          (if (number? a)
            (if (number? b)
              (compare a b)
              1)
            (if (number? b)
              -1
              (compare a b))))
        coll))

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

(defn pairs+
  "Pairs up ops from each process in a history. Yields a lazy sequence of [info]
  or [invoke, ok|fail|info] pairs. The difference from `pairs` is that this
  variant maps invocations to infos."
  ([history]
   (pairs+ {} history))
  ([invocations [op & ops]]
   (lazy-seq
     (when op
       (let [p (:process op)]
         (case (:type op)
           :invoke      (do (assert (not (contains? invocations p)))
                            (pairs+ (assoc invocations p op) ops))
           :info        (if (contains? invocations p)
                          (cons [(get invocations p) op]
                                (pairs+ (dissoc invocations p) ops))
                          (cons [op] (pairs+ invocations ops)))
           (:ok :fail)  (do (assert (contains? invocations p))
                            (cons [(get invocations p) op]
                                  (pairs+ (dissoc invocations p)
                                          ops)))))))))

(defn pair-index
  "Given a history, constructs a map from operations to their
  counterparts--invocations to their completions or completions to their
  invocations. Infos map to nil."
  ([history]
   (pair-index pairs history))
  ([pair-fn history]
   (->> history
        pair-fn
        (reduce (fn [index [invoke complete]]
                  ; We need these to be unique! Otherwise the index we build
                  ; won't be bijective
                  (assert (:index invoke))
                  (assoc! (if complete
                            (assoc! index complete invoke)
                            index)
                          invoke complete))
                (transient {}))
        persistent!)))

(defn pair-index+
  "Like pair-index, but matches invokes to infos as well."
  [history]
  (pair-index pairs+ history))

(defn invocation
  "Returns the invocation for an op, using a pair index. If the op is itself a
  completion, returns the op. If the op is an invocation, looks up its
  completion in the pair index."
  [pair-index op]
  (if (op/invoke? op)
    op
    (pair-index op)))

(defn completion
  "Returns the completion for an op, using a pair index. If the op is itself a
  completion, returns the op. If the op is an invocation, looks up its
  completion in the pair index."
  [pair-index op]
  (if (op/invoke? op)
    (pair-index op)
    op))

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
          _           (assert i (str "Process completed an operation without a "
                                     "prior invocation: "
                                     (pr-str op)))
          invocation  (nth history i)
          value       (:value op)
          invocation' (assoc invocation :value value)]
      [(-> history
           (assoc! i invocation')
           (conj! op))
       (dissoc! index (:process op))])

    ; A failure; fill in either value.
    :fail
    (let [i           (get index (:process op))
          _           (assert i (str "Process failed an operation without a "
                                     "prior invocation: "
                                     (pr-str op)))
          invocation  (nth history i)
          _           (assert (= (:value op) (:value invocation))
                              (str "invocation value "
                                   (pr-str (:value invocation))
                                   " and failure value "
                                   (pr-str (:value op))
                                   " don't match"))
          invocation' (assoc invocation :value (:value op), :fails? true)]
      [(-> history
           (assoc! i invocation')
           (conj!    op))
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

(defn indexed?
  "Is the given history indexed?"
  [history]
  (or (empty? history)
      (integer? (:index (first history)))))


(defn with-synthetic-infos
  "Histories may arrive with :invoke operations that never complete. We append
  synthetic :info ops to the end of the history for any in-process calls."
  [history]
  (assert (vector? history))
  (->> (unmatched-invokes history)
       (map (fn [invoke] (assoc invoke :type :info)))
       (into history)))

(defn without-failures
  "Takes a completed history, and returns a copy of the given history without
  any failed operations--either invocations or completions."
  [history]
  (->> history
       (remove (fn [op]
                 (or (op/fail? op)
                     (:fails? op))))
       vec))

(defn without-infos
  "Takes a completed history, and returns a copy of that history without any
  :info operations."
  [history]
  (->> history
       (remove op/info?)
       vec))
