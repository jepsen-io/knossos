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

(defn processes-test [ps]
  (let [op {:process 0 :index 0}]
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

(deftest map-processes-test
  (processes-test (map-processes)))

(deftest memoized-map-processes-test
  (processes-test (memoized-map-processes)))

(deftest array-processes-test
  (processes-test (array-processes [{:process 0 :index 0}])))

(deftest array-processes-search-test
  (testing "empty"
    (is (= -1 (array-processes-search (int-array []) 3))))

  (testing "one"
    (is (= -1 (array-processes-search (int-array [1 11]) 0)))
    (is (= 0  (array-processes-search (int-array [1 11]) 1)))
    (is (= -3 (array-processes-search (int-array [1 11]) 2))))

  (testing "two"
    (is (= -1 (array-processes-search (int-array [1 11 3 -33]) 0)))
    (is (= 0  (array-processes-search (int-array [1 11 3 -33]) 1)))
    (is (= -3 (array-processes-search (int-array [1 11 3 -33]) 2)))
    (is (= 2  (array-processes-search (int-array [1 11 3 -33]) 3)))
    (is (= -5 (array-processes-search (int-array [1 11 3 -33]) 4)))))
