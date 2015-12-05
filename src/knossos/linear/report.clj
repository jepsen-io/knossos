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

(defn condense-time-coords
  "Takes time coordinates (a map of op indices to [start-time end-time]), and
  condenses times to remove sparse regions."
  [coords]
  (let [mapping (->> coords
                     vals
                     (apply concat)
                     (into (sorted-set))
                     (map-indexed (fn [i coord] [coord i]))
                     (into {}))]
    (->> coords
         (map (fn [[k [t1 t2]]]
                [k [(mapping t1) (mapping t2)]]))
         (into {}))))

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
       (into {})
       condense-time-coords))

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
  (* x 150))

(defn vscale
  "Convert our units to vertical CSS pixels"
  [x]
  (* x 40))

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

(defn transition-color
  "What color should a transition be?"
  [transition]
  (if (model/inconsistent? (:model transition))
    "#C51919"
    "#000000"))

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

(defn hover-opacity
  "Takes an SVG element, makes it partially transparent, and adds onmouseover
  behavior raising its opacity."
  [e]
  (xml/add-attrs
    e
    :stroke-opacity "0.2"
    :onmouseover "this.setAttribute('stroke-opacity', '1.0');"
    :onmouseout  "this.setAttribute('stroke-opacity', '0.2');"))

(defn model-offset
  "Where should we position this model?"
  [model-numbers model]
  (-> model
      model-numbers
      (/ (count model-numbers))
      (* 0.75)
      (+ 1/8)))

(defn path-bounds
  "Assign an initial :min-x and :max-x to every transition in a path."
  [{:keys [time-coords]} path]
  (map (fn [transition]
         (let [op      (:op transition)
               [t1 t2] (time-coords (:index op))]
           (assoc transition
                  :min-x t1
                  :x t1
                  :max-x t2)))
       path))

(defn constrain-path
  "Applies constraints to a path, ensuring x coordinates are sequential and lie
  within the bounds of their parent operations."
  [path]
  (let [[ok? _ path] (reduce (fn [[ok? last-x path]
                                  {:keys [min-x x max-x] :as transition}]
                               (let [x' (cond (<= x min-x)  (+ min-x 0.1)
                                              (<= max-x x)  (- max-x 0.1)
                                              (<= x last-x) (+ last-x 0.1)
                                              true          x)]
                                 [(and ok? (= x x'))
                                  x'
                                  (conj path (assoc transition :x x'))]))
                             [true Double/NEGATIVE_INFINITY []]
                             path)]
    (if ok?
      path
      (recur path))))

(defn relax-path
  "Takes a single path and relaxes xs closer to their means."
  [path]
  (let [path   (transient (vec path))
        ; Relax final index to the maximum possible
        path   (if (zero? (count path))
                 path
                 (let [tfinal (nth path (dec (count path)))]
                   (assoc! path (dec (count path))
                           (assoc tfinal :x (:max-x tfinal)))))]
    (loop [i     (- (count path) 2)
           path  path]
      (if (zero? i)
        (persistent! path)
        (let [t0 (nth path (dec i))
              t1 (nth path i)
              t2 (nth path (inc i))]
          (recur (dec i)
                 (assoc! path i (assoc t1 :x (/ (+ (:x t0) (:x t2)) 2)))))))))

(defn relax-paths
  "We need a data structure that represents the visual layout of paths through
  operations, in a way that allows us to iteratively relax the layout into
  something more readable *while* preserving topological constraints. We'll
  augment an analysis path with :min-x, :max-x, and :x coordinates, and
  gradually manipulate :x given:

  Constraints:

    - Models must remain within the bounds of their parent operation
    - Models must lie between their previous and successor models in every path
      that touches them

  Relaxation:

    - Collapse identical models on the same op when possible
    - Push distinct models on an operation apart
    - Move models towards the mean of their neighbors."
  [paths]
  (loop [i     10
         paths paths]
    (if (zero? i)
      paths
      (recur (dec i)
             (->> paths
                  (map relax-path)
                  (map constrain-path))))))

(defn render-path
  "Renders a particular path, given learnings. Returns {:transitions, :models},
  each an SVG group. We render these separately because models go *on top* of
  ops, and transitions go *below* them."
  ([learnings path]
   (prn :render-path path)
   (render-path learnings nil path [] []))
  ([{:keys [time-coords process-coords pair-index model-numbers] :as learnings}
    [prev-x prev-y]
    path
    transition-svgs
    model-svgs]
   (if (empty? path)
     ; Done
     {:models      (-> (apply svg/group model-svgs) hover-opacity)
      :transitions (-> (apply svg/group transition-svgs)
                       hover-opacity)}
     ; Handle this transition
     (let [[transition & path'] path
           op      (:op transition)
           model   (:model transition)
           x       (:x transition)
           y       (process-coords (:process op))
           ; A line from previous coords to current coords
           line    (when prev-x
                     ; Are we going up or down in process space?
                     (let [up? (< prev-y y)
                           y0  (if up? (+ prev-y process-height) prev-y)
                           y1  (if up? y      (+ y process-height))]
                       (svg/line (hscale prev-x) (vscale y0)
                                 (hscale x)      (vscale y1)
                                 :stroke-width (vscale 0.1)
                                 :stroke (transition-color transition))))
           transition-svgs    (if line
                                (conj transition-svgs line)
                                transition-svgs)
           ; A vertical line for the model
           bar      (svg/line (hscale x) (vscale y)
                             (hscale x) (vscale (+ y process-height))
                             :stroke-width (vscale 0.1)
                             :stroke (transition-color transition))
           ; A little illustration of the model state
           bubble   (svg/group
                      (-> (svg/text (str model))
                          (xml/add-attrs :x (hscale x)
                                         :y (vscale (- y 0.1)))
                          (svg/style :fill (transition-color transition)
                                     :font-size (vscale (* process-height 0.5))
                                     :font-family "sans"
                                     :alignment-baseline :bottom
                                     :text-anchor :middle)))
           model-svgs (conj model-svgs bar bubble)]
       (recur learnings [x y] path' transition-svgs model-svgs)))))

(defn render-paths
  "Renders all paths from learnings. Returns {:models ..., :transitions ...}"
  [learnings]
  (let [paths (->> learnings
                   :analysis
                   :final-paths
                   (map (partial path-bounds learnings))
                   relax-paths
                   (mapv (partial render-path learnings)))]
    {:models      (apply svg/group (map :models       paths))
     :transitions (apply svg/group (map :transitions  paths))}))

(defn svg-2
  "Emits an SVG 2 document."
  [& args]
  (let [svg-1 (apply svg/svg args)]
    (xml/set-attrs svg-1
                   (-> (xml/get-attrs svg-1)
                       (assoc "version" "1.0")))))

(defn render-analysis!
  "Render an entire analysis."
  [history analysis file]
  (let [learnings                     (learnings history analysis)
        ops                           (render-ops learnings)
        paths                         (render-paths learnings)
        {:keys [models transitions]}  paths]
    (spit file
          (xml/emit
            (svg-2
              (-> (svg/group
                    transitions
                    ops
                    models)
                  (svg/translate (vscale 1) (vscale 1))))))))
