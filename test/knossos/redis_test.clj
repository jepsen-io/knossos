(ns knossos.redis-test
  (:require [clojure.test :refer :all]
            [knossos.core :refer [complete
                                  linearizations
                                  linearizable-prefix
                                  ->Register]]
            [knossos.redis :refer :all]
            [clojure.pprint :refer [pprint]]))

(defn trajectory
  "Returns a system from a randomized trajectory through the state space."
  [system depth]
  (if (zero? depth)
    system
    (let [possibilities (step system)]
      (if (empty? possibilities)
        ; Dead end
        system
        ; Descend
        (recur (rand-nth possibilities)
               (dec depth))))))

(deftest redis-test
  (dotimes [i 10000]
    (let [system (trajectory (system) 30)]
      ; Is this system linearizable?
      (let [history (complete (:history system))
            linears (linearizations (->Register nil) history)]
        (when (empty? linears)
          (let [linearizable (linearizable-prefix (->Register nil) history)]
            (println "No linearizable history for...")
            (pprint system)
            (println "History:")
            (pprint linearizable)
            (println "Unable to linearize past this point!")
            (pprint (drop (count linearizable) history))))


        (is (not (empty? linears)))))))
