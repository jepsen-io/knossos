(ns knossos.prioqueue-test
  (:require [clojure.test :refer :all]
            [knossos.prioqueue :refer :all]))

(deftest empty-test
  (is (nil? (poll! (prioqueue) 0)))
  (is (nil? (poll! (prioqueue) 10))))

(defn drain!
  "Drains elements from a queue to a seq."
  [q]
  (take-while (complement nil?) (repeatedly #(poll! q 0))))

(defn fill!
  "Fills a queue with [prio, element] pairs."
  ([coll]
   (fill! (prioqueue) coll))
  ([q coll]
   (doseq [[prio e] coll]
     (put! q prio e))
   q))

(deftest simple-test
  (is (= (drain! (fill! [[1 :a] [0 :b] [2 :c] [-1 :d]]))
         [:d :b :a :c])))

(deftest prioqueue-test
  (let [q  (prioqueue)
        w1 [1 2]
        w2 [3 4 5]]
    (->> [w1 w2 w1 w2]
         (map #(put! q (count %) %))
         (dorun))
    (is (= (take-while identity (repeatedly #(poll! q 1)))
           [w1 w1 w2 w2]))))
