(ns knossos.wgl
  "An implementation of the Wing and Gong linearizability checker, plus Lowe's
  extensions, as described in

  http://www.cs.cmu.edu/~wing/publications/WingGong93.pdf
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf
  https://arxiv.org/pdf/1504.00204.pdf"
  (:require [knossos [history :as history]
                     [op :as op]
                     [model :as model]]
            [knossos.wgl.dll-history :as dllh]
            [clojure.pprint :refer [pprint]])
  (:import (knossos.wgl.dll_history INode)
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

(defrecord CacheConfig [linearized model])

(defn max-entry-id
  "What's the highest entry ID in this history?"
  [history]
  (first (keep :entry-id (rseq history))))

(defn check
  [model history]
  (let [history     (-> history
                        history/complete
                        history/index
                        history/without-failures
                        with-entry-ids)
        n           (max-entry-id history)
        head-entry  (dllh/dll-history history)
        linearized  (BitSet. n)
        cache       #{}
        calls       (ArrayDeque.)]
    (loop [s          model
           cache      cache
           entry      ^INode (.next head-entry)]
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
            {:valid? true}

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
                      cache'      (conj cache (CacheConfig. linearized' s'))]
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
              {:valid? false
               :op    (.op entry)}

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
