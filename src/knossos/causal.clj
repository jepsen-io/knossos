(ns knossos.causal
  "BEGH causal consistency checker, from
  'On Verifying Causal Consistency'
  Bouajjani, Enea, Guerraoui, and Hamza
  https://arxiv.org/pdf/1611.00580.pdf"
  (:require [knossos
             [analysis :as analysis]
             [history :as history]
             [op :as op]
             [search :as search]
             [util :refer [deref-throw]]
             [model :as model]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]))

(defn completed-co?
  "Ensures attempted invocations completed and we have a CO with
  precisely 5 operations."
  [attempted completed]
  (let [ca (count attempted)
        cc (count completed)]
    (and (= ca cc)
         (= cc 5))))

(defn check
  "A series of causally consistent (CC) ops are a causal order (CO). We issue a CO
  of 5 read (r) and write (w) operations (r w r w r) against a register (key).
  All operations in this CO must appear to execute in the order provided by
  the issuing site (process). We also look for anomalies, such as unexpected values"
  [model history state]
  (let [attempted (filter op/invoke? history)
        completed (filter op/ok? history)]
    (if-not (completed-co? attempted completed)
      ;; This is to ensure a malformed CO does not invalidate a set of
      ;; tests results.
      {:valid? true
       :cause "CO incomplete"}
      (loop [s model
             history completed]
        (cond
          ;; Manually aborted
          (not (:running? @state))
          {:valid? :unknown
           :cause (:cause @state)}

          ;; We've checked every operation in the history
          (empty? history)
          {:valid? true
           :model s}

          true
          (let [op (first history)
                s' (model/step s op)]
            (if (model/inconsistent? s')
              {:valid? false
               :error (:msg s')}
              (recur s' (rest history)))))))))

(defn start-analysis
  "Spawns a search to check if a history is causally consistent"
  [model history]
  (let [state (atom {:running? true})
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
        [:BEGH-causal])

      (results [_]
        (deref-throw results))
      (results [_ timeout timeout-val]
        (deref-throw results timeout timeout-val)))))

(defn analysis
  [model history]
  (assoc (search/run (start-analysis model history))
         :analyzer :causal))
