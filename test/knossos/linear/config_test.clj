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
