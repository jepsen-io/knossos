(ns knossos.causal
  "BEGH causal consistency checker, from
  'On Verifying Causal Consistency'
  Bouajjani, Enea, Guerraoui, and Hamza
  https://arxiv.org/pdf/1611.00580.pdf"
  (:require [knossos [analysis :as analysis]
             [history :as history]
             [op :as op]
             [search :as search]
             [util :refer [deref-throw]]
             [model :as model]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]))

(defn write-co-init-read?
  "Looks for a violation of our initial read of 0. Could indicate
  test corruption where a client writes occurs against a
  CO's register."
  [r c]
  (when (= 0 c)
    (not= r c)))

(defn thin-air-read?
  "The CO register reads out a value other than init, w1, or w2"
  [r _]
  (not (some #{0 1 2} #{r})))

(defn write-co-read?
  "w2 appears before w1"
  [r c]
  (not= r c))

(defn check
  "A series of causally consistent (CC) ops are a causal order (CO). We issue a CO
  of 5 read (r) and write (w) operations (r w r w r) against a register (key).
  All operations in this CO must appear to execute in the order provided by
  the issuing site (process). We also look for anomalies, such as unexpected values"
  [model history state]
  (let [history (-> history
                    history/ensure-indexed
                    history/parse-ops
                    history/complete
                    history/without-failures)
        ;; _ (pprint history)
        ]
    (loop [s model
           op (first history)]
      (cond
        (not (:running? @state))
        {:valid? :unknown
         :cause (:cause @state)}

        ;; We've checked every operation in the history
        (not (next ))
        {:valid? true
         :model s}

        true
        (recur (.step s op)
               (next history))))))

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

(defn anaylsis
  [model history]
  (assoc (search/run (start-analysis model history))
         :analyzer :causal))
