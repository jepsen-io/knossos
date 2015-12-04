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
  (:require [clojure.pprint :refer [pprint]]
            [knossos [history :as history]
                     [op :as op]
                     [core :as core]
                     [model :as model]]
            [analemma [xml :as xml]
                      [svg :as svg]]))

(defn ops
  "Computes distinct ops in an analysis."
  [analysis]
  (->> analysis
       :final-paths
       (mapcat (partial map :op))
       distinct))

(defn models
  "Computes distinct models in an analysis."
  [analysis]
  (->> analysis
       :final-paths
       (mapcat (partial map :model))
       distinct))

(defn model-numbers
  "A map which takes models and returns an integer."
  [models]
  (->> models (map-indexed (fn [i x] [x i])) (into {})))

(defn process-coords
  "Given a set of operations, computes a map of process identifiers to their
  track coordinates 0, 2, 4, ..."
  [ops]
  (->> ops
       history/processes
       history/sort-processes
       (map-indexed (fn [i process] [process (* 2 i)]))
       (into {})))

(defn time-bounds
  "Given a pair index and an analysis, computes the [lower, upper] bounds on
  times for rendering a plot."
  [pair-index analysis]
  [(dec (or (:index (history/invocation pair-index (:previous-ok analysis)))
            1))
   (inc (:index (history/completion pair-index (:op analysis))))])

(defn time-coords
  "Takes a pair index, time bounds, and a set of ops. Returns a map of op
  indices to logical [start-time end-time] coordinates."
  [pair-index [tmin tmax] ops]
  (->> ops
       (map (fn [op]
              (let [i   (:index op)
                    _   (assert i)
                    t1  (max tmin (:index (history/invocation pair-index op)))
                    t2  (or (:index (history/completion pair-index op))
                            tmax)]
                [i [(- t1 tmin)
                    (- t2 tmin)]])))
       (into {})))

(defn learnings
  "What a terrible function name. We should task someone with an action item to
  double-click down on this refactor.

  Basically we're taking an analysis and figuring out all the stuff we're gonna
  need to render it."
  [history analysis]
  (let [history         (history/index (history/complete history))
        pair-index      (history/pair-index history)
        ops             (ops analysis)
        models          (models analysis)
        model-numbers   (model-numbers models)
        process-coords  (process-coords ops)
        time-bounds     (time-bounds pair-index analysis)
        time-coords     (time-coords pair-index time-bounds ops)]
    {:history       history
     :analysis      analysis
     :pair-index    pair-index
     :ops           ops
     :models        models
     :model-numbers model-numbers
     :process-coords process-coords
     :time-bounds   time-bounds
     :time-coords   time-coords}))

(def process-height
  "How tall should an op be in process space?"
  0.8)

(defn hscale
  "Convert our units to horizontal CSS pixels"
  [x]
  (* x 50))

(defn vscale
  "Convert our units to vertical CSS pixels"
  [x]
  (* x 20))

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
  "Given learnings, renders all operations as a group of SVG tags."
  [{:keys [time-coords process-coords pair-index ops]}]
  (->> ops
       (mapv (fn [op]
              (let [[t1 t2] (time-coords    (:index op))
                    p       (process-coords (:process op))
                    width   (- t2 t1)]
                (prn :rendering op :to [t1 t2] p)
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
       (apply svg/group)))

(defn render-path
  "Renders a particular path, given learnings."
  ([learnings path]
   (prn :render-path path)
   (render-path learnings nil path []))
  ([{:keys [time-coords process-coords pair-index model-numbers] :as learnings}
    [prev-x prev-y]
    path
    svg]
   (if (empty? path)
     ; Done
     (apply svg/group svg)
     (let [[transition & path'] path
           op      (:op transition)
           model   (:model transition)
           [t1 t2] (time-coords (:index op))
           p       (process-coords (:process op))
           x       (max t1 (inc (or prev-x -1)))
           y       p
           ; A line from previous coords to current coords
           line    (when prev-x
                     (svg/line (hscale prev-x) (vscale prev-y)
                               (hscale x)      (vscale y)
                               :stroke "#000000"))
           svg'    (if line (conj svg line) svg)]
       (recur learnings [x y] path' svg')))))

(defn render-paths
  "Renders all paths from learnings."
  [learnings]
  (->> learnings
       :analysis
       :final-paths
       (mapv (partial render-path learnings))
       (apply svg/group)))

(defn render-analysis!
  "Render an entire analysis."
  [history analysis]
  (let [learnings (learnings history analysis)]
    (spit "out.svg"
          (xml/emit
            (svg/svg
              (-> (svg/group
                    (render-ops   learnings)
                    (render-paths learnings))
                  (svg/translate (vscale 1) (vscale 1))))))))
