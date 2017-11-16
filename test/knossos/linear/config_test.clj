(ns knossos.linear.config-test
  (:require [clojure.test :refer :all]
            [knossos.linear.config :refer :all]
            [knossos.linear :refer [t-call t-lin t-ret]]
            [knossos.model :refer [register]]
            [knossos.op :refer :all])
  (:import (knossos.linear.config Processes)))


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

(deftest config-test
  (testing "kw lookups"
    (let [c (config :foo [])]
      (is (= :foo (:model c)))
      (isa? Processes (:processes c))))

  (testing "assoc"
    (let [c (config :foo [])]
      (is (= :a (:model     (assoc c :model     :a))))
      (is (= :b (:processes (assoc c :processes :b))))))

  (testing "equality"
    (let [c1 (assoc (config {1 2} []) :processes [3 4])
          c2 (assoc (config {1 2} []) :processes [3])
          c3 (assoc (config {2 3} []) :processes [3 4])]
      (is (= c1 (assoc c1 :model (:model c1))))
      (is (= c2 (assoc c2 :model (:model c2))))
      (is (= c3 (assoc c3 :model (:model c3))))
      (is (not= c1 c2))
      (is (not= c1 c3))
      (is (not= c2 c3)))))

(deftest config->map-test
  (let [w1 {:index 0 :process 0 :type :invoke :f :write :value 1}
        r2 {:index 1 :process 1 :type :invoke :f :read  :value 2}
        indices {0 nil
                 1 nil}
        h  [w1 r2]
        c  (config (register 0) h)]
    (testing "initial"
      (is (= {:model (register 0)
              :last-op nil
              :pending []}
             (config->map indices c))))

    (testing "pending"
      (let [c (-> c (t-call w1))]
        (is (= {:model (register 0)
                :last-op nil
                :pending [{:process 0 :type :invoke :f :write :value 1}]}
               (config->map indices c)))))

    (testing "linearized"
      (let [c (-> c (t-call w1) (t-lin w1))]
        (is (= {:model (register 1)
                :last-op {:process 0 :type :invoke :f :write :value 1}
                :pending []}
               (config->map indices c)))))))
