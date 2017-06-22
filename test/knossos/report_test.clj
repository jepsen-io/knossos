(ns knossos.report-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [knossos.linear :as linear]
            [knossos.wgl :as wgl]
            [knossos.linear.report :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [register cas-register]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(defn report!
  "Analyzes a file using the given analyzer & model, writing a corresponding
  file to report/"
  [analyzer model file]
  (let [history   (ct/read-history-2 file)
        analysis  (analyzer model history)
        hist-name ((re-find #"/([^/]+)\.edn$" file) 1)
        analyzer-name (->> analyzer
                           type
                           .getName
                           (re-find #"knossos\.(.+?)\$")
                           second)]
    (render-analysis! history analysis
                      (str "report/" hist-name "-" analyzer-name ".svg"))))

(deftest bad-analysis-test
  (report! linear/analysis (register 0)
           "data/cas-register/bad/bad-analysis.edn"))

(deftest rethink-analysis
  (report! linear/analysis (cas-register 0)
           "data/cas-register/bad/rethink-fail-smaller.edn"))

(deftest cas-failure
  (report! linear/analysis (cas-register nil)
           "data/cas-register/bad/cas-failure.edn"))

(deftest bad-analysis-test-wgl
  (report! wgl/analysis (register 0)
           "data/cas-register/bad/bad-analysis.edn"))

(deftest rethink-analysis-wgl
  (report! wgl/analysis (cas-register 0)
           "data/cas-register/bad/rethink-fail-smaller.edn"))

(deftest cas-failure-wgl
  (report! wgl/analysis (cas-register nil)
           "data/cas-register/bad/cas-failure.edn"))
