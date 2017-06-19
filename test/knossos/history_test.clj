(ns knossos.history-test
  (:require [knossos.history :refer :all]
            [clojure.test :refer :all]
            [knossos.op :as op]))

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
                      (op/fail   :a :read nil)])
           [(assoc (op/invoke :a :read nil) :fails? true)
            (op/fail   :a :read nil)])))

  (testing "an unbalanced set of invocations"
    (is (thrown? RuntimeException
                 (complete [(op/invoke :a :read nil)
                            (op/invoke :a :read nil)]))))

  (testing "an unbalanced completion"
    (is (thrown? AssertionError
                 (complete [(op/ok :a :read 2)])))))

(deftest pair-index+-test
  (testing "ok, crash, fail"
    (let [i1 (op/invoke 1 :write 1)
          i2 (op/invoke 2 :write 2)
          i3 (op/invoke 3 :write 3)
          c3 (op/fail   3 :write 3)
          c2 (op/info   2 :write 2)
          c1 (op/ok     1 :write 1)
          h  [i1 i2 i3 c3 c2 c1]]
      (is (= {i1 c1
              i2 c2
              i3 c3
              c1 i1
              c2 i2
              c3 i3}
             (pair-index+ h))))))

