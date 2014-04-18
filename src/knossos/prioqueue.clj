(ns knossos.prioqueue
  "A high-performance priority queue where only approximate ordering is
  required."
  (:require [clojure.tools.logging :refer :all])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.util.concurrent PriorityBlockingQueue
                                 BlockingQueue
                                 TimeUnit)))

(defprotocol Queue
  (put!  [q element])
  (poll! [q timeout]))

(defrecord StripedBlockingQueue [qs]
  Queue
  (put! [q element]
    ; Spray out to a random queue
    ; (.put ^BlockingQueue (rand-nth qs) element)
    ; Push onto our preferred queue plus one
    (let [t (mod (.. Thread currentThread getId) (count qs))]
      (.put ^BlockingQueue (nth qs t) element)))

  (poll! [q timeout]
    (let [n (count qs)
          t (mod (.. Thread currentThread getId) n)]
      ; Try our preferred queue
      (or (.poll ^BlockingQueue (nth qs t) timeout TimeUnit/MILLISECONDS)
          ; Then rotate through the others
          (loop [i 0]
            (when (< i n)
              (or (.poll ^BlockingQueue (nth qs (mod (+ t i) n)))
                  (recur (inc i)))))))))

(defn prioqueue [comparator]
  (StripedBlockingQueue.
    (->> (repeatedly #(PriorityBlockingQueue. 1 comparator))
         (take 12)
         vec)))
