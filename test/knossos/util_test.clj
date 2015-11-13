(ns knossos.util-test
  (:require [clojure.test :refer :all]
            [clojure.core.typed :refer [check-ns]]
            [knossos.util :refer :all]))

(deftest rkeep-test
  (testing "empty"
    (is (= [] (into [] (rkeep even? [])))))

  (testing "evens"
    (is (= [2 4] (into [] (rkeep #(when (even? %) %) [1 2 3 4]))))))
