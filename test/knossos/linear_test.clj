(ns knossos.linear-test
  (:require [clojure.test :refer :all]
            [knossos.linear :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [register]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest volatile-linearizable-test
  (dotimes [i 1]
    (let [history (ct/volatile-history 100 10 1/1000)
          _       (prn (count history))
          a       (analysis (register 0) history)]
      (is (:valid? a))
      (pprint a)
      (when-not (:valid? a)
        (pprint history)
        (println "history length" (count history))
        (prn)
        (pprint a)))))
