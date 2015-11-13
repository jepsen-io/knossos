(ns knossos.util
  "Toolbox"
  (:require [clojure.core.reducers :as r]
            [clojure.core.typed :refer [All
                                        ann
                                        ann-form
                                        EmptySeqable
                                        IFn
                                        List
                                        Seqable
                                        Set
                                        NonEmptySeqable
                                        Value]]
            [clojure.set :as set])
  (:import [clojure.lang Reduced]
           [clojure.core.protocols CollReduce]))

; Pretend reducibles are seqable: the noble lie of core.typed
(ann ^:no-check rempty? (All [a] [(Seqable a) -> Boolean
                                  :filters {:then (is (EmptySeqable a) 0)
                                            :else (is (NonEmptySeqable a) 0)}]))
(defn rempty?
  "Like empty, but for reducibles."
  [coll]
  (reduce (fn [_ _] (reduced false))
          true coll))

; oh i get it we're not *supposed* to implement reducers ourselves cool thx
(defmacro defcurried
  [name doc meta args & body]
  (#'r/do-curried name doc meta args body))

(defmacro rfn
  [[f1 k] fkv]
  (#'r/do-rfn f1 k fkv))

(defcurried rkeep
  "Like keep for reducers"
  {}
  [f coll]
  (r/folder coll
          (fn [f1]
            (rfn [f1 k]
                 ([ret k v]
                  (let [value (f k v)]
                    (if (nil? value)
                      ret
                      (f1 ret value))))))))

(ann ^:no-check foldset (All [a] [(Seqable a) -> (Set a)]))
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
