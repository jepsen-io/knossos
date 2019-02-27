(ns knossos.competition-test
  (:require [clojure.test :refer :all]
            [knossos.competition :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [cas-register register multi-register
                                   inconsistent]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest timeout-test
  (testing "will timeout if limit exceeded"
    (let [model (cas-register 0)
          history (ct/read-history-2 "data/cas-register/good/memstress3-12.edn")
          ;; Time limit of 1ms
          a (analysis model history {:time-limit 1})]
      (is (= {:valid?   :unknown
              :cause    :timeout}
             a))))

  (testing "does not timeout when limit not exceeded"
    (let [model (register 0)
          history [(invoke 0 :write 1)
                   (ok     0 :write 1)]
          a (analysis model history {:time-limit 999999999999999})]
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a))))

  (testing "does not timeout with no time-limit"
    (let [model (register 0)
          history [(invoke 0 :write 1)
                   (ok     0 :write 1)]
          a1 (analysis model history)
          a2 (analysis model history {:time-limit nil})]
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a1))
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a2)))))

(deftest ^:perf example-test
  (ct/test-examples analysis))
