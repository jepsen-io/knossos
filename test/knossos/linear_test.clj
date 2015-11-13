(ns knossos.linear-test
  (:require [clojure.test :refer :all]
            [knossos.linear :refer :all]
            [knossos.op :refer :all]
            [knossos.core :refer [->Register]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest volatile-linearizable-test
  (dotimes [i 1]
    (let [history (ct/volatile-history 100 1000 1/1000)
          _       (prn (count history))
          a       (analysis (->Register 0) history)]
      (is (:valid? a))
      (when-not (:valid? a)
        (pprint history)
        (println "history length" (count history))
        (prn)
        (pprint a)))))
