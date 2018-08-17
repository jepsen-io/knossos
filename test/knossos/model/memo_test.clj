(ns knossos.model.memo-test
  (:require [clojure.test :refer :all]
            [knossos.history :as history]
            [knossos.model :refer [register inconsistent step inconsistent?]]
            [knossos.model.memo :refer :all]
            [clojure.pprint :refer [pprint]])
  (:import knossos.model.Model))

(defn equiv-classes
  "Groups a sequence into collections of = elements."
  [coll]
  (vals (reduce (fn [classes x]
                  (assoc classes x (conj (classes x) x)))
                {}
                coll)))

(deftest canonical-history-test
  (let [history (take 100 (repeatedly (fn [] {:process (rand-int 5)
                                              :f       (str (rand-int 5))
                                              :value   [(rand-int 5)
                                                        (rand-int 5)]})))
        history' (canonical-history history)
        canonical? (fn [[x & more]]
                        (every? #(identical? x %) more))]
    (is (= history history'))
    (is (not (every? canonical? (equiv-classes (map :f history)))))
    (is (not (every? canonical? (equiv-classes (map :value history)))))
    (is (every? canonical? (equiv-classes (map :f history'))))
    (is (every? canonical? (equiv-classes (map :value history'))))))

(let [history (->> [{:type :invoke :process 0 :f :read :value :unreachable}
                    {:type :invoke :process 1 :f :read :value 1}
                    {:type :invoke :process 2 :f :write :value 1}
                    {:type :invoke :process 3 :f :write :value 2}
                    {:type :info   :process 4 :f :bad :value :bad}]
                   history/index)]

  (deftest transitions-test
    (is (= [{:f :read :value :unreachable}
            {:f :read :value 1}
            {:f :write :value 1}
            {:f :write :value 2}]
           (transitions history))))

  (deftest models-test
    (is (= #{(register 0)
             (register 1)
             (register 2)
             (inconsistent "0≠1")
             (inconsistent "0≠:unreachable")
             (inconsistent "1≠:unreachable")
             (inconsistent "2≠1")
             (inconsistent "2≠:unreachable")}
           (models (transitions history) (register 0)))))

  (deftest wrapper-test
    (let [w (wrapper (register 0) history)]
      (is (integer? (hash w)))
      (is (integer? (.hashCode w)))

      (is (= (register 0) (model w)))
      (is (= (register 1) (-> w (step (history 2)) model)))
      (is (= (register 1) (-> w
                              (step (history 2))
                              (step (history 1))
                              model)))
      (is (= (register 2) (-> w
                              (step (history 2))
                              (step (history 1))
                              (step (history 3))
                              model)))
      (is (= (inconsistent "2≠1") (-> w
                                      (step (history 3))
                                      (step (history 1)))))

      (is (= (-> w
                 (step (history 2))
                 (step (history 1))
                 (step (history 3)))
             (-> w
                 (step (history 2))
                 (step (history 3))
                 (step (history 3)))))
      )))


(defrecord Counter [value]
  Model
  (step [c op]
    (assert (= :inc (:f op)))
    (Counter. (+ value (:value op)))))

(deftest large-state-space-test
  ; Ordinarily, a counter like this would expand to an infinite collection of
  ; counters 0, 1, 2, ... if we tried to memoize it. We're verifying that we
  ; detect and bail on this case.
  (let [model   (Counter. 0)
        history [{:process 0, :type :invoke, :f :inc, :value 1}
                 {:process 0, :type :ok, :f :inc, :value 1}]
        memo (memo model history)]
    (is (identical? model (:model memo)))
    (is (= history (:history memo)))))
