(ns knossos.analysis
  "Shared functions for performing forensic reconstruction of a nonlinearizable
  history's final moments."
  (:require [knossos [history :as history]
                     [op :as op]
                     [model :as model]
                     [util :refer :all]]
            [clojure.core.reducers :as r]
            [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [knossos.model.memo :as memo :refer [memo]]))

(defn previous-oks
  "Computes a sequence of operations up to but not including the given
  operation."
  [history op]
  (->> history
       (take-while (partial not= op))
       (filter op/ok?)))

(defn previous-ok
  "Given a history and an operation, looks backwards in the history to find
  the previous ok. Returns nil if there was none."
  [history op]
  (assert (op/ok? op))
  (loop [i (dec (:index op))]
    (when-not (neg? i)
      (when-let [op (nth history i)]
        (if (op/ok? op)
          op
          (recur (dec i)))))))

(defn extend-path
  "Given a path (a sequence of {:model ... :op} maps) and some operations,
  applies those operations to extend the path until hitting an inconsistent
  point."
  [prefix ops]
  (reduce (fn [path op]
            (let [model (:model (peek path))
                  model' (model/step model op)
                  path'  (conj path {:op op :model model'})]
              (if (model/inconsistent? model')
                (reduced path')
                path')))
          prefix
          ops))

(defn final-paths-for-config
  "Returns a set of final paths for a specific configuration, given a
  collection of concurrent calls."
  [prefix final calls]
  (let [; First, identify the set of pending operations *other* than our
        ; final linearization.
        final-process (:process final)]
    ; Take all pending calls
    (->> calls
         ; Except the final call
         (r/filter (fn excluder [op] (not (= final-process (:process op)))))
         ; Now compute all permutations of those pending ops
         (into [])
         combo/subsets
         (r/mapcat combo/permutations)
         ; followed by the final op
         (r/map #(concat % (list final)))
         ; And extend the prefix along those paths
         (r/map (partial extend-path prefix))
         ; Computing a set
         (foldset))))
