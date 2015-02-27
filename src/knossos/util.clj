(ns knossos.util
  "Toolbox"
  (:require [clojure.core.reducers :as r]
            [clojure.core.typed :refer [ann
                                        ann-form
                                        IFn
                                        List
                                        Seqable
                                        Set
                                        Value]]
            [clojure.core.typed.unsafe :as unsafe]
            [clojure.set :as set])
  (:import [clojure.lang Reduced]
           [clojure.core.protocols CollReduce]))

(ann ^:no-check rempty? [CollReduce -> Boolean])
(defn rempty?
  "Like empty, but for reducibles."
  [coll]
  (reduce (fn [_ _] (reduced false))
          true coll))

(ann ^:no-check foldset [CollReduce -> Set])
(defn foldset
  "Folds a reducible collection into a set."
  [coll]
  (r/fold (r/monoid set/union hash-set)
          conj
          coll))

(ann maybe-list (All [x]
                     (IFn [nil -> (Value ())]
                          [x   -> (List x)])))
(defn maybe-list
  "If x is nil, returns the empty list. If x is not-nil, returns (x)."
  [x]
  (if x (list x) '()))

(defmacro with-thread-name
  "Sets the thread name for duration of block."
  [thread-name & body]
  `(let [old-name# (.. Thread currentThread getName)]
     (try
       (.. Thread currentThread (setName (name ~thread-name)))
       ~@body
       (finally (.. Thread currentThread (setName old-name#))))))
