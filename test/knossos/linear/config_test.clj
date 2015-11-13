(ns knossos.linear.config-test
  (:require [clojure.test :refer :all]
            [knossos.linear.config :refer :all]
            [knossos.op :refer :all]))

(deftest set-config-set-test
  (let [s (set-config-set)]
    (testing "empty"
      (is (empty? s))
      (is (= 0 (count s)))
      (is (= nil (seq s))))

    (testing "adding"
      (add! s 1)
      (add! s 2)
      (add! s 1)
      (is (not (empty? s)))
      (is (= 2 (count s)))
      (is (= #{1 2} (set s))))))

(deftest processes-test
  (let [ps (map-processes)
        op {:process 0}]
    (testing "empty"
      (is (= [] (into [] (calls ps))))
      (is (idle? ps 0))
      (is (not (calling? ps 0)))
      (is (not (returning? ps 0))))

    (testing "calling"
      (let [ps (call ps op)]
        (is (= [op] (into [] (calls ps))))
        (is (not (idle? ps 0)))
        (is (calling? ps 0))
        (is (not (calling? ps 1)))
        (is (not (returning? ps 0)))))

    (testing "linearizing"
      (let [ps (-> ps (call op) (linearize op))]
        (is (= [] (into [] (calls ps))))
        (is (not (idle? ps 0)))
        (is (not (calling? ps 0)))
        (is (returning? ps 0))
        (is (not (returning? ps 1)))))

    (testing "returning"
      (let [ps (-> ps (call op) (linearize op) (return op))]
        (is (= [] (into [] (calls ps))))
        (is (idle? ps 0))))
        (is (not (calling? ps 0)))
        (is (not (returning? ps 0)))))
