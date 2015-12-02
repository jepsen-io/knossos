(ns knossos.linear.report
  "Constructs reports from a linear analysis.

  When a linear analyzer terminates it gives us a set of configs, each of which
  consists of a model, a final operation, and a set of pending operations. Our
  job is to render those operations like so:

              +---------+ +--------+
      proc 0  | write 1 | | read 0 |
              +---------+ +--------+
                    +---------+
      proc 1        | write 0 |
                    +---------+

      ------------ time ---------->"
  (:require [knossos [history :as history]
                     [op :as op]]
            [analemma [xml :as xml]
                      [svg :as svg]]))

(defn process-coords
  "Given a set of operations, computes a map of process identifiers to their
  track coordinates 0, 2, 4, ..."
  [ops]
  (->> ops
       history/processes
       history/sort-processes
       (map-indexed (fn [i process] [process (* 2 i)]))
       (into {})))

(defn time-coords
  "Takes a pair index and a set of operations from the history that generated
  it. Returns a map of indices to logical [start-time end-time] coordinates."
  [pair-index ops]
  (let [tmax (->> ops
                  (map (fn [op]
                         (:index (or (history/completion pair-index op)
                                     op))))
                  (reduce max 0))]
    (->> ops
         (map (fn [op]
                 (let [t1 (:index op)
                       _  (assert t1)
                       t2 (:index (pair-index op))]
                   [t1 (if t2
                         (sort (list t1 t2))
                         (list t1 tmax))])))
         (into {}))))

(def process-height
  "How tall should an op be in process space?"
  0.8)

(defn hscale
  "Convert our units to horizontal CSS pixels"
  [x]
  (* x 100))

(defn vscale
  "Convert our units to vertical CSS pixels"
  [x]
  (* x 50))

(def type->color
  {:ok   "#B3F3B5"
   nil   "#F2F3B3"
   :fail "#F3B3B3"})

(defn op-color
  "What color should an op be?"
  [pair-index op]
  (-> pair-index
      (history/completion op)
      :type
      type->color))

(defn render-ops
  "Given a history and some operations from it, renders that history as svg
  tags."
  [history ops]
  (let [process-coords (process-coords ops)
        pair-index     (history/pair-index history)
        time-coords    (time-coords pair-index ops)
        tmin           (->> time-coords
                            vals
                            (map first)
                            (reduce min Double/POSITIVE_INFINITY))
        tmax           (->> time-coords
                            vals
                            (map second)
                            (reduce max 0))
        pmin           (reduce min Double/POSITIVE_INFINITY
                               (vals process-coords))
        pmax           (reduce max 0 (vals process-coords))]
    (->> ops
         (map (fn [op]
                (let [[t1 t2] (time-coords    (:index op))
                      p       (process-coords (:process op))
                      width   (- t2 t1)]
                  (svg/group
                    (svg/rect (hscale t1)
                              (vscale p)
                              (vscale process-height)
                              (hscale width)
                              :rx (hscale 0.1)
                              :ry (hscale 0.1)
                              :fill (op-color pair-index op))
                    (-> (svg/text (str (name (:f op)) " "
                                       (pr-str (:value op))))
                        (xml/add-attrs :x (hscale (+ t1 (/ width 2.0)))
                                       :y (vscale (+ p (/ process-height
                                                          2.0))))
                        (svg/style :fill "#000000"
                                   :font-size (vscale (* process-height 0.6))
                                   :font-family "sans"
                                   :alignment-baseline :middle
                                   :text-anchor :middle))))))
         (apply svg/group))))

(defn render-model
  "Render one particular model's possibilities."
  [history model ops]
  (spit "out.svg"
        (-> (render-ops history ops)
            (svg/transform "scale(0.5)")
            svg/svg
            xml/emit)))

(defn render-analysis!
  "Render an entire analysis."
  [history analysis]
  (let [history     (history/index (history/complete history))
        pair-index  (history/pair-index history)
        final-op    (history/invocation pair-index (:op analysis))
        ; Reach backwards in time to the previous OK op. This is where the
        ; linear analyzer obtained its current set of models.
        first-op    (loop [i (dec (:index (:op analysis)))]
                      (when-let [op (nth history i)]
                        (if (op/ok? op)
                          op
                          (recur (dec i)))))
        ; Where should we start the plot? Just before the first OK's
        ; invocation.
        tmin        (if first-op
                      (dec (:index (pair-index first-op)))
                      0)
        ; Where should we end the plot? At the time of the final ok completion.
        tmax        (if final-op
                      (:index final-op)
                      (count history))
        ; Group models together
        models->configs (group-by :model (:configs analysis))]
    (doseq [[model configs] models->configs]
      (let [ops (->> configs (mapcat :pending) set)
            ops (if final-op (conj ops final-op) ops)
            ops (if first-op (conj ops first-op) ops)]
        (prn :ops ops)
        (render-model history model ops)))))
