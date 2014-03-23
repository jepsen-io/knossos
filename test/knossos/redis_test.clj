(ns knossos.redis-test
  (:require [clojure.test :refer :all]
            [knossos.core :refer [complete
                                  linearizations
                                  linearizable-prefix
                                  ->Register]]
            [knossos.redis :refer :all]
            [knossos.core-test :refer [dothreads]]
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

(defn print-system
  [system history]
  (let [linearizable (linearizable-prefix (->Register nil) history)]
    (locking *out*
      (println "\n\n### No linearizable history for system ###\n")
      (pprint (dissoc system :history))
      (println "\nHistory:\n")
      (pprint linearizable)
      (println "\nUnable to linearize past this point!\n")
      (pprint (drop (count linearizable) history)))))

(deftest redis-test
  (dothreads [t 48] 
    (dotimes [i 10000]
      (let [system (trajectory (system) 15)]
        ; Is this system linearizable?
        (let [history (complete (:history system))
              linears (linearizations (->Register nil) history)]
          (when (empty? linears)
            (print-system system history))
          (is (not (empty? linears))))))))
