(ns knossos.linear-test
  (:require [clojure.test :refer :all]
            [knossos.linear :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [cas-register register multi-register
                                   inconsistent]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest crash-test
  (let [model (register 0)
        history [(invoke 0 :write 1)
                 (info   0 :write 1)]
        a (analysis model history)]
    (is (:valid? a))))

(deftest sequential-test
  (let [model (register 0)
        history [(invoke 0 :read 0)
                 (ok     0 :read 0)]
        a (analysis model history)]
    (is (:valid? a))))

(deftest preserves-indices-test
  (let [model (register 0)
        history [{:process 0 :type :invoke :f :write :value 1 :index 5}
                 {:process 0 :type :ok     :f :write :value 1 :index 7}
                 {:process 0 :type :invoke :f :read :value 2 :index 99}
                 {:process 0 :type :ok     :f :read :value 2 :index 100}]
        a (analysis model history)]
    (is (not (:valid? a)))
    (is (= 100 (get-in a [:op :index])))
    (is (= 7   (get-in a [:last-op :index])))
    (is (= 7   (get-in a [:previous-ok :index])))
    (let [configs (-> a :configs first)]
      (is (= 99 (-> configs :pending first :index)))
      (is (= 7  (-> configs :last-op :index))))
    (let [final-paths (-> a :final-paths vec first)]
      (is (= 7   (-> final-paths first  :op :index)))
      (is (= 100 (-> final-paths second :op :index))))))

(deftest read-wrong-initial-value-test
  (let [model (register 0)
        history [{:process 0 :type :invoke :f :read :value 1}
                 {:process 0 :type :ok     :f :read :value 1}]
        a       (analysis model history)]
    (is (not (:valid? a)))
    (is (= {:process 0, :type :ok, :f :read, :value 1 :index 1}
           (:op a)))
    (is (= #{[{:op nil, :model model}
              {:op {:process 0 :type :ok :f :read :value 1 :index 1},
               :model (inconsistent "0≠1")}]},
           (:final-paths a)))
    (is (not (:last-op a)))
    (is (not (:previous-ok a)))))

(deftest bad-analysis-test
  (let [model (register 0)
        history [{:process 0 :type :invoke :f :read :value 1}
                 {:process 0 :type :ok     :f :read :value 1}]
        a (analysis model history)]
    (is (= :linear (:analyzer a)))
    (is (not  (:valid? a)))
    (is (nil? (:previous-ok a)))
    (is (nil? (:last-op a)))
    (is (= {:process 0
            :type    :ok
            :f       :read
            :value   1
            :index   1}
         (:op a)))
    (is (= [{:model model
           :last-op nil
           :pending [{:process  0
                      :type     :invoke
                      :f        :read
                      :value    1
                      :index    0}]}]
           (:configs a)))
    (is (= #{[{:model model :op nil}
              {:model (inconsistent "0≠1")
               :op {:process 0 :type :ok :f :read :value 1 :index 1}}]}
         (:final-paths a)))))

(deftest bad-analysis-test-2
  (let [model (cas-register 0)
        a (analysis model (ct/read-history-2 "data/cas-register/bad/cas-failure.edn"))]
    ; In this particular history, we know the value is 0, then we have
    ; concurrent reads of 0 and a write of 2 by process 76, followed by another
    ; read of 0 by process 70. The only legal linearization to that final read
    ; is all reads of 0, followed by 76 write 2, which leaves the state as 2.
    ; Process 70 read 0 should be the invalidating op.
    (is (not (:valid? a)))
    ;; This is the only possible state at this time.
    (is (= [{:model (cas-register 2)
             :last-op {:f :write :process 76 :type :ok :value 2 :index 472}
             :pending
             [{:process 70 :type :invoke :f :read :value 0 :index 488}
              {:process 77 :type :invoke :f :cas :value [1 1] :index 463}]}]
           (:configs a)))
    ; We fail because we can't linearize the final read of 0
    (is (= {:process 70 :type :ok :f :read :value 0 :index 491}
           (:op a)))
    ; The last linearized ok was the completion of process 74's read of 0, but
    ; that's not the last linearized *op*: that'd be the write of 2.
    (is (= {:process 74 :type :ok :f :read :value 0 :index 478}
           (:previous-ok a)))
    (is (= {:process 76 :type :ok :f :write :value 2 :index 472}
           (:last-op a)))

    ; There are two possible options: write 2, CAS 1->1, or write 2, read 0.
    (is (= #{[{:model (cas-register 2)
               :op {:f :write :process 76 :type :ok :value 2 :index 472}}
              {:model (inconsistent "can't CAS 2 from 1 to 1")
               :op {:f :cas :process 77 :type :invoke :value [1 1] :index 463}}]
             [{:model (cas-register 2)
               :op {:f :write :process 76 :type :ok :value 2 :index 472}}
              {:model (inconsistent "can't read 0 from register 2")
               :op {:f :read :process 70 :type :ok :value 0 :index 491}}]}
           (:final-paths a)))))

(deftest volatile-linearizable-test
  (dotimes [i 10]
    (let [history (ct/volatile-history 100 50 1/1000)
          _       (prn (count history))
          a       (analysis (register 0) history)]
      (is (:valid? a))
      (when (not= true (:valid? a))
        (println "history length" (count history))
        (prn)
        (pprint (assoc a :configs (take 2 (:configs a))))))))

(deftest rethink-fail-minimal-test
  (let [a (analysis (cas-register nil)
                    (ct/read-history-2 "data/cas-register/bad/rethink-fail-minimal.edn"))]
    (is (= false (:valid? a)))

    ; We shouldn't be able to linearize the read of 3 by process 12, which
    ; appears out of nowhere.
    (is (= {:process 1 :type :ok :f :read :value 3 :index 4}
           (:op a)))))

(deftest cas-failure-test
  (let [a (analysis (cas-register 0)
                    (ct/read-history-2 "data/cas-register/bad/cas-failure.edn"))]
    (is (= false (:valid? a)))
    (is (= {:f :read :process 70 :type :ok :value 0 :index 491}
           (:op a)))))

(deftest multi-register-test
  (let [a (analysis (multi-register {:x 0 :y 0})
                    (ct/read-history-2 "data/multi-register/good/multi-register.edn"))]
    (is (:valid? a))
    (is (= #{(multi-register {:x 2 :y 2})
             (multi-register {:x 2 :y 0})}
           (set (map :model (:configs a)))))))

(deftest example-test
  (ct/test-examples analysis))
