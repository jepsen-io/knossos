(ns knossos.linear-test
  (:require [clojure.test :refer :all]
            [knossos.linear :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [register]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest bad-analysis-test
  (let [history [{:process 0 :type :invoke :f :read :value 1}
                 {:process 0 :type :ok     :f :read :value 1}]]
    (is (= {:valid? false
            :op {:process 0
                 :index   1
                 :type    :ok
                 :f       :read
                 :value   1}
            :configs [{:model (register 0)
                       :pending [{:process  0
                                  :index    0
                                  :type     :invoke
                                  :f        :read
                                  :value    1}]}]}
           (analysis (register 0) history)))))

(deftest volatile-linearizable-test
  (dotimes [i 1]
    (let [history (ct/volatile-history 10 10 1/1000)
;          _       (prn (count history))
          a       (analysis (register 0) history)]
      (is (:valid? a))
;      (pprint a)
      (when-not (:valid? a)
        (pprint history)
        (println "history length" (count history))
        (prn)
        (pprint a)))))
