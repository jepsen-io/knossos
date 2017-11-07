(ns knossos.op-test
  (:require [clojure.test :refer :all]
            [knossos.op :refer :all]))

(deftest type-test
  (let [op-op     (op 0 :invoke :read 1)
        invoke-op (invoke 0 :read 1)
        ok-op     (ok 0 :write 1)
        fail-op   (fail 0 :cas 1)
        info-op   (info 0 :read nil)]
    (is (= (:index op-op) -1))
    (is (= (:type invoke-op) :invoke))
    (is (= (:type ok-op) :ok))
    (is (= (:type fail-op) :fail))
    (is (= (:type info-op) :info))))

(deftest support-test
  (let [ok-op (ok 0 :read 1)
        invoke-op (invoke 0 :read 1)
        fail-op (fail 0 :read 1)
        info-op (info 0 :read 1)
        process-op1 (map->Op {:process 0 :type :invoke :f :read :value 1})
        process-op2 (map->Op {:process 0 :type :ok :f :read :value 2})
        process-op3 (map->Op {:process :nemsis :type :info :f :start :value nil})]
    (is (ok? ok-op))
    (is (invoke? invoke-op))
    (is (fail? fail-op))
    (is (info? info-op))
    (is (same-process? process-op1 process-op2))
    (is (not (same-process? process-op1 process-op3)))))
