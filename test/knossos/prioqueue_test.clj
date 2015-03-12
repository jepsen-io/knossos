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
