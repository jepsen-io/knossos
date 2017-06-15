(ns knossos.wgl.dll-history-test
  (:require [clojure.test :refer :all]
            [knossos.wgl.dll-history :as d]
            [knossos.op :as op]
            [knossos.history :as history]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest roundtrip-test
  (testing "empty"
    (is (= nil
           (seq (d/dll-history [])))))

  (testing "one element"
    (is (= [1 1]
           (map :value (d/dll-history
                         (history/index [(op/invoke 0 :read 1)
                                         (op/ok 0 :read 1)]))))))

  (testing "more"
    (is (= [1 2 3 1 2 3]
           (map :value (d/dll-history
                         (history/index [{:process 0 :type :invoke, :value 1}
                                         {:process 1 :type :invoke, :value 2}
                                         {:process 2 :type :invoke, :value 3}
                                         (op/info 0 :read 1)
                                         (op/info 1 :read 2)
                                         (op/info 2 :read 3)])))))))

(deftest structure-match-test
  (let [h (history/index [{:process 0 :type :invoke :f :read}
                          {:process 1 :type :invoke :f :write}
                          {:process 1 :type :ok     :f :write}
                          {:process 2 :type :invoke :f :crash}
                          {:process 2 :type :info   :f :crash}
                          {:process 0 :type :ok     :f :read}])
        [o1 o2 o3 o4 o5 o6] h
        h (d/dll-history h)
        [n0 n1 n2 n3 n4 n5 n6] (d/node-seq h)]
    (testing "next"
      (is (= n1 (.next n0)))
      (is (= n2 (.next n1)))
      (is (= n3 (.next n2)))
      (is (= n4 (.next n3)))
      (is (= n5 (.next n4)))
      (is (= n6 (.next n5)))
      (is (nil? (.next n6))))

    (testing "prev"
      (is (nil? (.prev n0)))
      (is (= n0 (.prev n1)))
      (is (= n1 (.prev n2)))
      (is (= n2 (.prev n3)))
      (is (= n3 (.prev n4)))
      (is (= n4 (.prev n5)))
      (is (= n5 (.prev n6))))

    (testing "op"
      (is (nil? (.op n0)))
      (is (= o1 (.op n1)))
      (is (= o2 (.op n2)))
      (is (= o3 (.op n3)))
      (is (= o4 (.op n4)))
      (is (= o6 (.op n5))) ; shifted up
      (is (= o5 (.op n6)))) ; reordered info

    (testing "match"
      (is (nil? (.match n0)))
      (is (= n5 (.match n1))) ; 6 shifted up when info 5 reordered
      (is (= n3 (.match n2)))
      (is (nil? (.match n3)))
      (is (= n6 (.match n4))) ; info 5 reordered to end
      (is (nil? (.match n5)))
      (is (nil? (.match n6))))))
