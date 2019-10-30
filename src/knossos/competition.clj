(ns knossos.competition
  "A checker which uses both WGL and linear analyses, terminating as soon as one has a decision."
  (:require [knossos [search :as search]
                     [linear :as linear]
                     [wgl :as wgl]]))

(defn analysis
  "Given an initial model state and a history, checks to see if the history is
  linearizable by running both the :linear and :wgl analyzers.
  Returns a map with a :valid? bool and additional debugging information.

  Can also take an options map:
  {:time-limit ms} Duration to wait before returning with result :unknown"
  ([model history]
   (analysis model history {}))
  ([model history opts]
   (search/run
     (search/competition {:linear (linear/start-analysis model history)
                          :wgl    (wgl/start-analysis model history)})
     opts)))
