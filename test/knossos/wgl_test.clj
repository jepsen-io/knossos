(ns knossos.wgl-test
  (:require [clojure.test :refer :all]
            [knossos.wgl :refer :all]
            [knossos.op :as op]
            [knossos.model :as model]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest read-wrong-initial-value-test
  (let [model (model/register 0)
        history [{:process 0 :type :invoke :f :read :value nil}
                 {:process 0 :type :ok     :f :read :value 1}]
        a       (analysis model history)]
    (is (not (:valid? a)))))

(deftest crash-test
  (let [model (model/register 0)
        history [(op/invoke 0 :write 1)
                 (op/info   0 :write 1)]
        a (analysis model history)]
    (is (:valid? a))))

(deftest sequential-test
  (let [a (analysis (model/register 0)
                    [(op/invoke 0 :read 0)
                     (op/ok     0 :read 0)])]
    (is (:valid? a))))

(deftest cas-failure-test
  (let [a (analysis (model/cas-register 0)
                    (ct/read-history "data/cas-failure.edn"))]
    (is (not (:valid? a)))))

(deftest volatile-linearizable-test
  (dotimes [i 10]
    (let [history (ct/volatile-history 100 50 1/1000)
          a       (analysis (model/register 0) history)]
      (is (:valid? a)))))
