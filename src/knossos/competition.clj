(ns knossos.competition
  "A checker which uses both WGL and linear analyses, terminating as soon as one has a decision."
  (:require [knossos [search :as search]
                     [linear :as linear]
                     [wgl :as wgl]]))

(defn analysis
  [model history]
  (search/run
    (search/competition
      {:linear (linear/start-analysis model history)
       :wgl    (wgl/start-analysis model history)})))
