(ns knossos.linear.report-test
  (:require [clojure.test :refer :all]
            [knossos.linear :as linear]
            [knossos.linear.report :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [register cas-register]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest bad-analysis-test
  (let [history [{:process 4, :type :invoke, :f :write, :value 2}
                 {:process 4, :type :ok, :f :write, :value 2}
                 {:process 6, :type :invoke, :f :read, :value nil}
                 {:process 1, :type :invoke, :f :write, :value 4}
                 {:process 1, :type :ok, :f :write, :value 4}
                 {:process 14, :type :invoke, :f :read, :value 4}
                 {:process 14, :type :ok, :f :read, :value 4}
                 {:process 9, :type :invoke, :f :write, :value 0}
                 {:process 9, :type :ok, :f :write, :value 0}
                 {:process 19, :type :invoke, :f :read, :value 0}
                 {:process 20, :type :invoke, :f :write, :value 1}
                 {:process 19, :type :ok, :f :read, :value 0}
                 {:process 22  :type :invoke, :f :read, :value 3}
                 {:process 21, :type :invoke, :f :read, :value 2}
                 {:process 21, :type :ok, :f :read, :value 2}]
        model    (register 0)
        analysis (linear/analysis model history)]
    (render-analysis! history analysis "out.svg")))

(deftest bad-analysis-test-2
  (let [history  (read-string (slurp "data/rethink-fail.edn"))
        model    (cas-register 0)
        analysis (linear/analysis model history)]
    (render-analysis! history analysis "rethink.svg")))
