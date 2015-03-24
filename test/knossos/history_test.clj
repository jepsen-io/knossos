(ns knossos.history-test
  (:require [knossos.history :refer :all]
            [clojure.test :refer :all]
            [clojure.core.typed :refer [check-ns]]
            [knossos.op :as op]))

(deftest typecheck
  (is (check-ns 'knossos.history)))

(deftest complete-test
  (testing "empty history"
    (is (= (complete [])
           [])))

  (testing "an invocation"
    (is (= (complete [(op/invoke :a :read nil)])
           [(op/invoke :a :read nil)])))

  (testing "a completed invocation"
    (is (= (complete [(op/invoke :a :read nil)
                      (op/ok     :a :read 2)])
           [(op/invoke :a :read 2)
            (op/ok     :a :read 2)])))

  (testing "a failed invocation"
    (is (= (complete [(op/invoke :a :read nil)
                      (op/fail   :a :read 2)])
           (complete [(op/invoke :a :read 2)
                      (op/fail   :a :read nil)])
           [(op/info   :a :read [:failed-invoke 2])
            (op/fail   :a :read 2)])))

  (testing "an unbalanced set of invocations"
    (is (thrown? RuntimeException
                 (complete [(op/invoke :a :read nil)
                            (op/invoke :a :read nil)]))))

  (testing "an unbalanced completion"
    (is (thrown? AssertionError
                 (complete [(op/ok :a :read 2)])))))
