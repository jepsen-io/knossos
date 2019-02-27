(ns knossos.search
  "Manages reporting and aborting searches"
  (:require [knossos.memory :as memory]
            [knossos.util :refer [with-thread-name]]
            [clojure.tools.logging :refer [info warn]]))

(defprotocol Search
  (abort!  [search cause])
  (report  [search])
  (results [search]
           [search timeout timeout-val]))

(defn results-from-any
  "Tries to deref any of a collection of Searches. If a period is provided,
  that's roughly how long in ms it should take to cycle through polling the
  collection."
  ([xs]
   (results-from-any xs 100))
  ([xs period]
   (results-from-any (vec xs) (long (/ period (count xs))) 0))
  ([xs period i]
   (let [res (results (nth xs i) period ::next)]
     (if (= res ::next)
       (recur xs period (mod (inc i) (count xs)))
       res))))

(defn competition
  "Takes a map of names to searches and creates a composed search which runs
  all of them in competition."
  [searches]
  (reify Search
    (abort! [_ cause]
      (doseq [s (vals searches)]
        (abort! s cause)))

    (report [_]
      (->> searches
           (map (fn [[name search]] [name (report search)]))
           (into {})))

    (results [this]
      (let [r (results-from-any (vals searches))]
        (abort! this :competition-ended)
        r))

    (results [this timeout timeout-val]
      (let [p   (promise)
            _   (future
                  (deliver p (results-from-any (vals searches))))
            r   (deref p timeout timeout-val)]
        (abort! this :competition-ended)
        r))))

(defn reporter
  "Spawns a reporting thread that periodically logs the state of the analysis,
  if it's different from where we last left off."
  [search interval]
  (let [running? (atom true)]
    (future
      (with-thread-name "knossos reporter"
        (loop [last-state nil]
          (Thread/sleep interval)
          (when @running?
            (let [state (try (report search)
                             (catch Throwable t
                               (warn t "Error reporting")))]
              (when (not= last-state state)
                (info state))
              (recur state))))))

    (fn kill! []
      (reset! running? false))))

(defn run
  "Given a Search, spins up reporting and memory-pressure handling, executes
  the search, and returns results.

  Can also take an options map:
  {:time-limit ms} Duration to wait before returning with result :unknown"
  ([search]
   (run search {}))
  ([search opts]
   (let [mem-watch (memory/on-low-mem!
                    (fn abort []
                      (warn "Out of memory; aborting search")
                      (abort! search :out-of-memory)))
         reporter  (reporter search 5000)
         time-limit (:time-limit opts)]
     (try
       (if time-limit
         (results search time-limit {:valid? :unknown :cause :timeout})
         (results search))
       (finally
         (mem-watch)
         (reporter))))))
