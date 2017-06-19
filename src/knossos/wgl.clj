(ns knossos.wgl
  "An implementation of the Wing and Gong linearizability checker, plus Lowe's
  extensions, as described in

  http://www.cs.cmu.edu/~wing/publications/WingGong93.pdf
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf
  https://arxiv.org/pdf/1504.00204.pdf"
  (:require [knossos [analysis :as analysis]
                     [history :as history]
                     [op :as op]
                     [model :as model]]
            [knossos.wgl.dll-history :as dllh]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set])
  (:import (knossos.wgl.dll_history Node
                                    INode)
           (java.util BitSet
                      ArrayDeque
                      Set)))

(defn with-entry-ids
  "Assigns a sequential :entry-id to every invocation operation."
  [history]
  (loop [h  (seq history)
         h' (transient [])
         i 0]
    (if-not h
      ; Done
      (persistent! h')

      (let [op (first h)]
        (if (op/invoke? op)
          (recur (next h)
                 (conj! h' (assoc op :entry-id i))
                 (inc i))
          (recur (next h) (conj! h' op) i))))))

(defrecord CacheConfig [linearized model op])

(defn max-entry-id
  "What's the highest entry ID in this history?"
  [history]
  (first (keep :entry-id (rseq history))))

(defn bitset-highest-linearized
  "What's the highest entry-id linearized by this BitSet?"
  [^BitSet b]
  (dec (.length b)))

(defn configurations-which-linearized
  "Filters a collection of configurations to only those which linearized the
  given entry-ids. More efficient if called with ascending-order entry IDs."
  [configs entry-ids]
  (let [entry-ids (int-array (reverse entry-ids))]
    (filter (fn [config]
              (loop [i 0]
                (if (<= (alength entry-ids) i)
                  true

                  (if (.get ^BitSet (:linearized config) (aget entry-ids i))
                    (recur (inc i))
                    false))))
            configs)))

(defn highest-linearized-entry-id
  "Finds the highest linearized entry id given a set of CacheConfigs. -1 if no
  entries linearized."
  [configs]
  (->> configs
       (map :linearized)
       (map bitset-highest-linearized)
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
         (map (fn [config]
                (let [linearized ^BitSet (:linearized config)
                      model (:model config)
                      calls (->> calls
                                 (remove (fn [call]
                                           (.get linearized (:entry-id call))))
                                 (mapv pair-index))]
                  (analysis/final-paths-for-config
                    [{:op    (pair-index (:op config))
                      :model model}]
                    final-op
                    calls))))
         (reduce set/union))))

(defn invalid-analysis
  "Constructs an analysis of an invalid terminal state."
  [history configs]
  (let [pair-index     (history/pair-index+ history)
        ; We failed because we ran into an OK entry. That means its invocation
        ; couldn't be linearized. What was the entry ID for that invocation?
        final-entry-id (inc (highest-linearized-entry-id configs))

        ; From that we can find the invocation and ok ops themselves
        [final-invoke
         final-ok]  (invoke-and-ok-for-entry-id history final-entry-id)

        ; Now let's look back to the previous ok we *could* linearize
        previous-ok (analysis/previous-ok history final-ok)
        previous-invoke (history/invocation (history/pair-index history)
                                            previous-ok)

        ; In general, we needed to linearize *every* previous OK to get here.
        previous-entry-ids  (->> final-ok
                                 (analysis/previous-oks history)
                                 (map pair-index)
                                 (map :entry-id))

        ; So what configurations linearized those entry IDs?
        configs     (configurations-which-linearized
                      configs previous-entry-ids)

        ; Now compute the final paths from the previous ok operation to the
        ; nonlinearizable one
        final-paths (final-paths history pair-index final-ok configs)]
    {:valid?      false
     :op          final-ok
     :previous-ok previous-ok
     :final-paths final-paths}))

(defn check
  [model history]
  (let [history     (-> history
                        history/complete
                        history/without-failures
                        history/index
                        with-entry-ids)
        n           (or (max-entry-id history) 0)
        head-entry  (dllh/dll-history history)
        linearized  (BitSet. n)
        cache       #{(CacheConfig. (BitSet.) model nil)}
        calls       (ArrayDeque.)]
    (loop [s              model
           cache          cache
           ^Node entry    (.next head-entry)]
      (if-not (.next head-entry)
        ; We've linearized every operation in the history
        {:valid? true
         :model  s}

        (let [op (.op entry)]
          (cond
            ; We reordered all the infos to the end in dll-history, which means
            ; that at this point, there won't be any more invoke or ok
            ; operations. That means there are no more chances to fail in the
            ; search, and we can conclude here. The sequence of :invoke nodes
            ; in our dll-history is exactly those operations we chose not to
            ; linearize, and each corresponds to an info here at the end of the
            ; history.
            (op/info? op)
            {:valid? true
             :model  s}

            ; This is an invocation. Can we apply it now?
            (op/invoke? op)
            (let [op (.op entry)
                  s' (model/step s op)]
              (if (model/inconsistent? s')
                ; We can't apply this right now; try the next entry
                (recur s cache (.next entry))

                ; OK, we can apply this to the current state
                (let [; Cache that we've explored this configuration
                      linearized' ^BitSet (.clone linearized)
                      _           (.set linearized' (:entry-id op))
                      cache'      (conj cache (CacheConfig. linearized' s' op))]
                  (if (identical? cache cache')
                    ; We've already been here, try skipping this invocation
                    (let [entry (.next entry)]
                      (recur s cache entry))

                    ; We haven't seen this state yet
                    (let [; Record this call so we can backtrack
                          _           (.addFirst calls [entry s])
                          ; Clean up
                          s           s'
                          _           (.set linearized (:entry-id op))
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
                    _                 (.set linearized ^long (:entry-id op) false)
                    _                 (dllh/unlift! entry)
                    entry             (.next entry)]
                (recur s cache entry)))))))))

(defn analysis
  [model history]
  (let [res (check model history)]
    res))
