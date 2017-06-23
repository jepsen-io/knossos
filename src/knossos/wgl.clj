(ns knossos.wgl
  "An implementation of the Wing and Gong linearizability checker, plus Lowe's
  extensions, as described in

  http://www.cs.cmu.edu/~wing/publications/WingGong93.pdf
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf
  https://arxiv.org/pdf/1504.00204.pdf"
  (:require [knossos [analysis :as analysis]
                     [history :as history]
                     [op :as op]
                     [search :as search]
                     [util :refer [deref-throw]]
                     [model :as model]]
            [knossos.wgl.dll-history :as dllh]
            [knossos.model.memo :as memo :refer [memo]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [potemkin :refer [definterface+ deftype+]])
  (:import (knossos.wgl.dll_history Node INode)
           (knossos.model.memo Wrapper)
           (java.util BitSet
                      ArrayDeque
                      Set)))

; This is the datatype we use to prune our exploration of the search space. We
; assume the linearized BitSet is immutable, memoizing its hashing to optimize
; both hash and equality checks. Op is only present for failure reconstruction
; and does not participate in equality/hashing checks.
(deftype+ CacheConfig [^BitSet linearized
                       model
                       op
                       ^:volatile-mutable ^int hasheq]
  clojure.lang.IHashEq
  (hasheq [ps]
          (when (= -1 hasheq)
            (set! hasheq (int (bit-xor (hash linearized)
                                       (hash model)))))
          hasheq)

  Object
  (equals [this other]
          (or (identical? this other)
              (and (instance? CacheConfig other)
                   (= hasheq      (.hasheq      ^CacheConfig other))
                   (= model       (.model       ^CacheConfig other))
                   (= linearized  (.linearized  ^CacheConfig other))))))

(defn linearized [^CacheConfig c]
  (.linearized c))

(defn cache-config [linearized model op]
  (CacheConfig. linearized model op -1))


(defn with-entry-ids
  "Assigns a sequential :entry-id to every invocation and completion. We're
  going to pick these ids in a sort of subtle way, because when it comes time
  to identify the point where we got stuck in the search, we want to be able to
  look at a linearized bitset and quickly identify the highest OK operation we
  linearized. To that end, we assign ok invocations the range 0..c, and crashed
  operations c+1..n, such that contiguous ranges of linearized ops represent
  progress through every required OK operation in the history."
  [history]
  (loop [h'     (transient []) ; New history
         calls  (transient {}) ; A map of processes to the indices of invokes.
         i      0              ; Index into the history
         e-ok   0              ; Next entry ID
         ; entry-ids for infos
         e-info (- (count history)
                   (count (history/crashed-invokes history)))]
    (if (<= (count history) i)
      ; OK, done!
      (do (assert (= 0 (count calls)))
          (persistent! h'))

      ; Normal op processing
      (let [op (nth history i)
            p  (:process op)]
        (cond (op/invoke? op)
              (do (assert (not (get calls p)))
                  (recur (assoc! h' i op)
                         (assoc! calls p i)
                         (inc i)
                         e-ok
                         e-info))

              (op/ok? op)
              (let [invoke-i (get calls p)
                    invoke (nth h' invoke-i)]
                (recur (-> h'
                           (assoc! invoke-i (assoc invoke :entry-id e-ok))
                           (assoc! i        (assoc op     :entry-id e-ok)))
                       (dissoc! calls p)
                       (inc i)
                       (inc e-ok)
                       e-info))

              (op/fail? op)
              (assert false (str "Err, shouldn't have a failure here: "
                                 (pr-str op)))

              (op/info? op)
              (if-let [invoke-i (get calls p)]
                ; This info corresponds to an ealier invoke
                (let [invoke (nth h' invoke-i)]
                  (recur (-> h'
                             (assoc! invoke-i (assoc invoke :entry-id e-info))
                             (assoc! i        (assoc op     :entry-id e-info)))
                         (dissoc! calls p)
                         (inc i)
                         e-ok
                         (inc e-info)))

                ; Just a random info
                (recur (assoc! h' i op)
                       calls
                       (inc i)
                       e-ok
                       e-info)))))))

(defn max-entry-id
  "What's the highest entry ID in this history?"
  [history]
  (reduce max -1 (keep :entry-id history)))

(defn bitset-highest-contiguous-linearized
  "What's the highest entry-id this BitSet has linearized, such that every
  lower entry ID is also linearized? -1 if nothing linearized."
  [^BitSet b]
  (dec (.nextClearBit b 0)))

(defn configurations-which-linearized-up-to
  "Filters a collection of configurations to only those which linearized every
  operation up to and including the given entry id."
  [entry-id configs]
  (filter (fn [^CacheConfig config]
            (<= entry-id
                (bitset-highest-contiguous-linearized (.linearized config))))
          configs))

(defn highest-contiguous-linearized-entry-id
  "Finds the highest linearized entry id given a set of CacheConfigs. -1 if no
  entries linearized. Because our search is speculative, we may actually try
  linearizing random high entry IDs even though we can't linearize smaller
  ones. For that reason, we use the highest *contiguous* entry ID."
  [configs]
  (->> configs
       (map linearized)
       (map bitset-highest-contiguous-linearized)
       (reduce max -1)))

(defn invoke-and-ok-for-entry-id
  "Given a history and an entry ID, finds the [invocation completion] operation
  for that entry ID."
  [history entry-id]
  (let [found (reduce (fn [invoke op]
                        (if invoke
                          (if (= (:process invoke) (:process op))
                            (reduced [invoke op])
                            invoke)
                          (if (= entry-id (:entry-id op))
                            op
                            invoke)))
                      nil
                      history)]
    (assert (vector? found))
    found))

(defn history-without-linearized
  "Takes a history and a linearized bitset, and constructs a sequence over that
  history with already linearized operations removed."
  [history ^BitSet linearized]
  (loop [history  (seq history)
         history' (transient [])
         calls    (transient #{})]
    (if-not history
      (persistent! history')
      (let [op       (first history)
            process  (:process op)
            entry-id (:entry-id op)]
        (cond ; This entry has been linearized; skip it and its completion.
              (and entry-id (.get linearized entry-id))
              (recur (next history) history' (conj! calls process))

              ; This entry is a return from a call that was already linearized.
              (get calls process)
              (recur (next history) history' (disj! calls process))

              ; Something else
              true
              (recur (next history) (conj! history' op) calls))))))

(defn concurrent-ops-at
  "Given a history and a particular operation in that history, computes the set
  of invoke operations concurrent with that particular operation."
  [history target-op]
  (loop [history    (seq history)
         concurrent (transient {})]
    (if-not history
      (vals (persistent! concurrent))
      (let [op      (first history)
            process (:process op)]
        (cond ; Found our target
              (= op target-op)
              (vals (persistent! concurrent))

              ; Start a new op
              (op/invoke? op)
              (recur (next history) (assoc! concurrent process op))

              ; Crashes
              (op/info? op)
              (recur (next history) concurrent)

              ; ok/fail
              true
              (recur (next history) (dissoc! concurrent process)))))))


(defn final-paths
  "We have a raw history, a final ok op which was nonlinearizable, and a
  collection of cache configurations. We're going to compute the set of
  potentially concurrent calls at the nonlinearizable ok operation.

  Then we'll filter the configurations to those which got the furthest into the
  history. Each config tells us the state of the system after linearizing a set
  of operations. We take that state and apply all concurrent operations to it,
  in every possible order, ending with the final op which broke
  linearizability."
  [history pair-index final-op configs]
  (let [calls (concurrent-ops-at history final-op)]
    (->> configs
         (map (fn [^CacheConfig config]
                (let [linearized ^BitSet (.linearized config)
                      model (.model config)
                      calls (->> calls
                                 (remove (fn [call]
                                           (.get linearized (:entry-id call))))
                                 (map pair-index)
                                 (mapv (fn [op] (dissoc op :entry-id))))]
                  (analysis/final-paths-for-config
                    [{:op    (dissoc (pair-index (:op config)) :entry-id)
                      :model model}]
                    (dissoc final-op :entry-id)
                    calls))))
         (reduce set/union)
         ; Unwrap memoization
         (map (fn [path]
                (mapv (fn [transition]
                        (let [m (:model transition)]
                          (if (instance? Wrapper m)
                            (assoc transition :model (memo/model m))
                            transition)))
                      path)))
         set)))

(defn invalid-analysis
  "Constructs an analysis of an invalid terminal state."
  [history configs]
  (let [pair-index     (history/pair-index+ history)
        ; We failed because we ran into an OK entry. That means its invocation
        ; couldn't be linearized. What was the entry ID for that invocation?
        final-entry-id (inc (highest-contiguous-linearized-entry-id configs))
        ;_ (prn :final-entry-id final-entry-id)

        ; From that we can find the invocation and ok ops themselves
        [final-invoke
         final-ok]  (invoke-and-ok-for-entry-id history final-entry-id)
        ;_ (prn :final-invoke final-invoke)
        ;_ (prn :final-ok     final-ok)

        ; Now let's look back to the previous ok we *could* linearize
        previous-ok (analysis/previous-ok history final-ok)

        configs (->> configs
                     (configurations-which-linearized-up-to
                       (dec final-entry-id)))
        ;_     (prn :configs)
        ;_     (pprint configs)

        ; Now compute the final paths from the previous ok operation to the
        ; nonlinearizable one
        final-paths (final-paths history pair-index final-ok configs)]
    {:valid?      false
     :op          (dissoc final-ok :entry-id)
     :previous-ok (dissoc previous-ok :entry-id)
     :final-paths final-paths}))

(defn check
  [model history state]
  (let [history     (-> history
                        history/complete
                        history/with-synthetic-infos
                        history/without-failures
                        history/index
                        with-entry-ids)
        {:keys [model history]} (memo model history)
        ; _ (pprint history)
        n           (max (max-entry-id history) 0)
        head-entry  (dllh/dll-history history)
        linearized  (BitSet. n)
        cache       #{(cache-config (BitSet.) model nil)}
        calls       (ArrayDeque.)]
    (loop [s              model
           cache          cache
           ^Node entry    (.next head-entry)]
        (cond
          ; Manual abort!
          (not (:running? @state))
          {:valid? :unknown
           :cause  (:cause @state)}

          ; We've linearized every operation in the history
          (not (.next head-entry))
          {:valid? true
           :model  s}

          true
          (let [op (.op entry)]
            (cond
              ; We reordered all the infos to the end in dll-history, which
              ; means that at this point, there won't be any more invoke or ok
              ; operations. That means there are no more chances to fail in the
              ; search, and we can conclude here. The sequence of :invoke nodes
              ; in our dll-history is exactly those operations we chose not to
              ; linearize, and each corresponds to an info here at the end of
              ; the history.
              (op/info? op)
              {:valid? true
               :model  s}

              ; This is an invocation. Can we apply it now?
              (op/invoke? op)
              (let [s' (model/step s op)]
                (if (model/inconsistent? s')
                  ; We can't apply this right now; try the next entry
                  (recur s cache (.next entry))

                  ; OK, we can apply this to the current state
                  (let [; Cache that we've explored this configuration
                        entry-id    (:entry-id op)
                        linearized' ^BitSet (.clone linearized)
                        _           (.set linearized' entry-id)
                        ;_           (prn :linearized op)
                        ; Very important that we don't mutate linearized' once
                        ; we've wrapped it in a CacheConfig!
                        cache'      (conj cache
                                          (cache-config linearized' s' op))]
                    (if (identical? cache cache')
                      ; We've already been here, try skipping this invocation
                      (let [entry (.next entry)]
                        (recur s cache entry))

                      ; We haven't seen this state yet
                      (let [; Record this call so we can backtrack
                            _           (.addFirst calls [entry s])
                            ; Clean up
                            s           s'
                            _           (.set linearized entry-id)
                            ; Remove this call and its return from the
                            ; history
                            _           (dllh/lift! entry)
                            ; Start over from the start of the shortened
                            ; history
                            entry       (.next head-entry)]
                        (recur s cache' entry))))))

              ; This is an :ok operation. If we *had* linearized the invocation
              ; already, this OK would have been removed from the chain by
              ; `lift!`, so we know that we haven't linearized it yet, and this
              ; is a dead end.
              (op/ok? op)
              (if (.isEmpty calls)
                ; We have nowhere left to backtrack to.
                (invalid-analysis history cache)

                ; Backtrack, reverting to an earlier state.
                (let [[^INode entry s]  (.removeFirst calls)
                      op                (.op entry)
                      ; _                 (prn :revert op :to s)
                      _                 (.set linearized ^long (:entry-id op)
                                              false)
                      _                 (dllh/unlift! entry)
                      entry             (.next entry)]
                  (recur s cache entry)))))))))

(defn start-analysis
  "Spawn a thread to check a history. Returns a Search."
  [model history]
  (let [state   (atom {:running? true})
        results (promise)
        worker  (Thread.
                  (fn []
                    (try
                      (deliver results
                               (check model history state))
                      (catch InterruptedException e
                        (let [{:keys [cause]} @state]
                          (deliver results
                                   {:valid? :unknown
                                    :cause  cause})))
                      (catch Throwable t
                        (deliver results t)))))]

    (.start worker)
    (reify search/Search
      (abort! [_ cause]
        (swap! state assoc
               :running? false
               :cause cause))

      (report [_]
        [:WGL])

      (results [_]
        (deref-throw results))
      (results [_ timeout timeout-val]
        (deref-throw results timeout timeout-val)))))

(defn analysis
  "Given an initial model state and a history, checks to see if the history is
  linearizable. Returns a map with a :valid? bool and additional debugging
  information."
  [model history]
  (search/run (start-analysis model history)))
