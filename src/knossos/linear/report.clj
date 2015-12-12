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
            [clojure.set :as set]
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

(defn path-bounds
  "Assign an initial :y, :min-x and :max-x to every transition in a path."
  [{:keys [time-coords process-coords]} path]
  (map (fn [transition]
         (let [op      (:op transition)
               [t1 t2] (time-coords (:index op))]
           (assoc transition
                  :y (process-coords (:process op))
                  :min-x t1
                  :x t1
                  :max-x t2)))
       path))

(defn paths
  "Given time coords, process coords, and an analysis, emits paths with
  coordinate bounds."
  [analysis time-coords process-coords]
  (->> analysis
       :final-paths
       (map (partial path-bounds {:time-coords    time-coords
                                  :process-coords process-coords}))))

(defn bounds-okay?
  [lines]
  (every? (fn [{:keys [min-x0 max-x0 min-x1 max-x1] :as line}]
            (or (nil? line)
                (and (every? (fn [id]
                               (let [l (get lines id)]
                                 (or (and (= (:min-x1 l) min-x0)
                                          (= (:max-x1 l) max-x0))
                                     (do (prn :mismatch-bounds)
                                         (prn l)
                                         (prn line)))))
                             (:prev line))
                     (every? (fn [id]
                               (let [l (get lines id)]
                                 (or (and (= (:min-x0 l) min-x1)
                                          (= (:max-x0 l) max-x1))
                                     (do (prn :mismatch-bounds)
                                         (prn line)
                                         (prn l)))))
                             (:next line)))))
          lines))

(defn path->line
  "Takes a path, a collection of lines, and emits a collection of lines with
  :min-x0, :max-x0, :y0, :min-x1, :max-x1, :y1 coordinate ranges, and an
  attached model for the termination point. Assigns a :line-id to each
  transition for the line which terminates there. Returns a tuple [path'
  lines'] where path' has line ids referring to indices in lines."
  [path lines]
  (let [first-line (count lines)
        [path lines] (reduce
                        (fn [[path lines min-x0 max-x0 y0] transition]
                          (let [min-x1 (min (:max-x transition)
                                            (max (+ min-x0 0.5)
                                                 (:min-x transition)))
                                max-x1 (:max-x transition)
                                y1     (:y     transition)]
                            (if (nil? y0)
                              ; First step
                              [(conj path transition)
                               lines
                               min-x1
                               max-x1
                               y1]
                              ; Recurrence
                              [(conj path (assoc transition
                                                 :line-id (count lines)))
                               (conj lines {:id      (count lines)
                                            :model   (:model transition)
                                            :min-x0  min-x0
                                            :max-x0  max-x0
                                            :y0      y0
                                            :min-x1  min-x1
                                            :max-x1  max-x1
                                            :y1      y1
                                            :prev    #{(dec (count lines))}
                                            :next    #{(inc (count lines))}
                                            })
                               min-x1
                               max-x1
                               y1])))
                        [[] lines Double/NEGATIVE_INFINITY nil nil]
                        path)
        lines (if (= first-line (count lines))
                ; We didn't add any lines
                lines
                ; Snip the first prev and last next pointers in the path
                (-> lines
                    (assoc-in [first-line :prev] #{})
                    (assoc-in [(dec (count lines)) :next] #{})))]
    [path lines]))

(defn paths->initial-lines
  "Takes a collection of paths and returns that collection of paths with each
  transition augmented with a :line-id, and a vector of lines indexed by
  line-id."
  [paths]
  (reduce (fn [[paths lines] path]
            (let [[path lines] (path->line path lines)]
              [(conj paths path) lines]))
          [[] []]
          paths))

(defn range-intersection
  "Computes the intersection [min max] of two ranges, [inclusive exclusive].
  Returns nil if no intersection exists."
  [min1 max1 min2 max2]
  (cond (<= max1 min2) nil
        (<= max2 min1) nil
        true [(max min1 min2)
              (min max1 max2)]))

(defn recursive-get
  "Looks up a key in a map recursively, by taking (get m k) and using it as a
  new key."
  [m k]
  (when-let [v (get m k)]
    (or (recursive-get m v)
        v)))

(defn collapse-mapping
  "Takes a map of x->x, where x may be a key in the map, and flattens it such
  that every key points directly to its final target."
  [m]
  (->> m
       keys
       (map (fn [k] [k (recursive-get m k)]))
       (into {})))

(defn ancestor?
  "Is a an ancestor of b?"
  ([lines a-id b-id]
   (ancestor? lines #{} a-id b-id))
  ([lines visited a-id b-id]
   (if (= a-id b-id)
     true
     (let [b (get lines b-id)
           _ (assert b)
           neighbors (:prev b)
           unvisited (remove visited neighbors)
           visited'  (into visited neighbors)]
       (some (partial ancestor? lines visited' a-id) unvisited)))))

(declare propagate-x1)

(defn propagate-x0
  "Propagate new x0 to a line and its predecessors' x1s and their
  successors' x0s and their predecessors' x1s and so on. Returns lines."
  ([lines id x]
   (first (propagate-x0 lines #{} id x)))
  ([lines visited id x]
   (let [line      (get lines id)
         lines     (assoc lines id (assoc line :x0 x))
         prevs     (:prev line)
         unvisited (remove visited prevs)
         visited   (into visited prevs)]
     (loop [lines     lines
            visited   visited
            unvisited unvisited]
       (if (empty? unvisited)
         [lines visited]
         (let [[lines visited] (propagate-x1
                                 lines visited (first unvisited) x)]
           (recur lines visited (next unvisited))))))))

(defn propagate-x1
  "Propagate new x1 to a line and its successors' x0s and their
  predecessors x1s and their successors x0s and so on. Returns lines."
  ([lines id x]
   (first (propagate-x1 lines #{} id x)))
  ([lines visited id x]
  (let [line      (get lines id)
        lines     (assoc lines id (assoc line :x1 x))
        nexts     (:next line)
        unvisited (remove visited nexts)
        visited   (into visited nexts)]
    (loop [lines     lines
           visited   visited
           unvisited unvisited]
      (if (empty? unvisited)
        [lines visited]
        (let [[lines visited] (propagate-x0
                                lines visited (first unvisited) x)]
          (recur lines visited (next unvisited))))))))

(declare propagate-x1-bounds)

(defn propagate-x0-bounds
  "Propagate new x0 values to a line and its predecessors' x1s and their
  successors' x0s and their predecessors' x1s and so on. Returns lines."
  ([lines id xmin xmax]
   (first (propagate-x0-bounds lines #{} id xmin xmax)))
  ([lines visited id xmin xmax]
   (let [line      (get lines id)
         lines     (assoc lines id (assoc line :min-x0 xmin, :max-x0 xmax))
         prevs     (:prev line)
         unvisited (remove visited prevs)
         visited   (into visited prevs)]
     (loop [lines     lines
            visited   visited
            unvisited unvisited]
       (if (empty? unvisited)
         [lines visited]
         (let [[lines visited] (propagate-x1-bounds
                                 lines visited (first unvisited) xmin xmax)]
           (recur lines visited (next unvisited))))))))

(defn propagate-x1-bounds
  "Propagate new x1 values to a line and its successors' x0s and their
  predecessors x1s and their successors x0s and so on. Returns lines."
  ([lines id xmin xmax]
   (first (propagate-x1-bounds lines #{} id xmin xmax)))
  ([lines visited id xmin xmax]
  (let [line      (get lines id)
        lines     (assoc lines id (assoc line :min-x1 xmin, :max-x1 xmax))
        nexts     (:next line)
        unvisited (remove visited nexts)
        visited   (into visited nexts)]
    (loop [lines     lines
           visited   visited
           unvisited unvisited]
      (if (empty? unvisited)
        [lines visited]
        (let [[lines visited] (propagate-x0-bounds
                                lines visited (first unvisited) xmin xmax)]
          (recur lines visited (next unvisited))))))))

(defn merge-two-lines
  "We're guided by a simple invariant: if the new set of lines satisfies the
  same [min-x max-x] ranges as the old set of lines, we're OK. To satisfy this
  constraint, we can only *narrow* ranges, never expand them.

  Consider two lines a and b which are mergeable:

         [aaaaaaaaaaaaaa]
      [bbbbbbbbbb]

         [aaaaa]
             [bbbbbb]

  Their merged line, c, satisfies both ranges.

         [aaaaaaa]aaaaaa]
      [bb[bbbbbbb]
         [ccccccc]

         [aaa[a]
             [b]bbbb]
             [c]

  Moreover, there's a line which can go from c0 to c1. If min-c0 were higher
  than max-c1, the line would no longer move forward in time and would be
  illegal.

  In general, a line has several prev and several next lines. When we merge
  lines, their next and prev sets are respectively merged.

      ╲ ╲|
       ╲||  previous lines must be able to land at any point in c0
        ╲|
      [ccccccc]

          [c]
           |╲     next lines must be able to emit from any point in c1
           | ╲

  We enforce that the termination ranges of all prev lines are equal to the
  beginning range of c, and all next lines have a starting range equal to the
  ending range of c. This means we must *propagate* the new starting range to
  the ending ranges of all previous lines, and vice versa.

  Finally, we must update the line id in all previous lines' next set, and all
  next lines prev set, to point to c."
  [lines mapping a b]
  (assert a)
  (assert b)
  ; Okay first things first: *can* we merge these?
  (let [[min0 max0] (range-intersection (:min-x0 a) (:max-x0 a)
                                        (:min-x0 b) (:max-x0 b))
        [min1 max1] (range-intersection (:min-x1 a) (:max-x1 a)
                                        (:min-x1 b) (:max-x1 b))]
    (if-not (and min0           ; The starts intersect
                 min1           ; The ends intersect
;                 (<= 1 (- min1 min0))
;                 (<= -1 (- max1 max0))
                 (<= 1 (- max1 min0)) ; Could this line still move forward
                 (= (:y0 a) (:y0 b)) ; Same starting process
                 (= (:y1 a) (:y1 b)) ; Same ending process
                 (= (:model a) (:model b)) ; Same resulting models
                 (not (ancestor? lines (:id a) (:id b)))  ; Not causally
                 (not (ancestor? lines (:id b) (:id a)))) ; connected
      ; Nope
      [lines mapping]
      ; OK merge
      (let [id    (count lines)
            nexts (set/union (:next a) (:next b))
            prevs (set/union (:prev a) (:prev b))
            line {:id     id
                  :model  (:model a)
                  :min-x0 min0
                  :max-x0 max0
                  :min-x1 min1
                  :max-x1 max1
                  :y0     (:y0 a)
                  :y1     (:y1 a)
                  :next   nexts
                  :prev   prevs}
            mapping' {(:id a) id
                      (:id b) id}
            ; Kill unused lines and add our new one
            lines (assoc lines
                         (:id a) nil
                         (:id b) nil
                         (:id line) line)
            ; Update next and prev references to a and b.
            lines (reduce (fn [lines prev-id]
                            (assert (not= prev-id (:id a)))
                            (assert (not= prev-id (:id b)))
                            (let [prev (get lines prev-id)
                                  _    (assert prev)
                                  prev-next (->> prev
                                                 :next
                                                 (map #(get mapping' % %))
                                                 set)]
                              (assoc lines prev-id (assoc prev
                                                          :next prev-next))))
                          lines
                          prevs)
            lines (reduce (fn [lines next-id]
                            (assert (not= next-id (:id a)))
                            (assert (not= next-id (:id b)))
                            (let [next (get lines next-id)
                                  _    (assert next)
                                  next-prev (->> next
                                                 :prev
                                                 (map #(get mapping' % %))
                                                 set)]
                              (assoc lines next-id (assoc next
                                                          :prev next-prev))))
                          lines
                          nexts)
            ; Propagate new bounds
            lines (propagate-x0-bounds lines id min0 max0)
            lines (propagate-x1-bounds lines id min1 max1)
            mapping (merge mapping mapping')]
        (prn :merging)
        (prn :a a)
        (prn :b b)
        (prn :c line)
        (assert (bounds-okay? lines))
        [lines mapping line]))))

(defn merge-lines-r
  "Given [a set of lines, a mapping], and a group of possibly-clusterable
  lines, merges as many as possible, returning [lines mapping].

  Our job here is to find some kind of minimal (whatever that means) set of
  non-mergeable lines. We'll start with an empty merged set, and gradually fold
  candidates into any of the merged set, or add them to the set if they can't
  merge with any."
  ([[lines mapping] candidates]
   ; We'll try to fold together lines roughly ordered by time.
   (merge-lines-r lines mapping #{}
                  (->> candidates
                       (sort-by (juxt :min-x0 :max-x0))
                       (map :id))))
  ([lines mapping merged candidates]
   (if (empty? candidates)
     ; Done
     [lines mapping]

     ; Try to merge into one of the merged lines
     (let [candidate (get lines (first candidates))
           [lines' mapping' merged']
           (loop [[m & more] (seq merged)]
             (if (nil? m)
               ; This candidate doesn't merge with anything in the merged set
               [lines mapping (conj merged (:id candidate))]
               ; Try merging
               (let [[lines' mapping' line]
                     (merge-two-lines lines mapping (get lines m) candidate)]
                 (if line
                   ; Merged!
                   [lines' mapping' (-> merged (disj m) (conj (:id line)))]
                   ; Nope
                   (recur more)))))]
       (recur lines' mapping' merged' (next candidates))))))

(defn merge-lines
  "Takes an associative collection of line-ids to lines and produces a new
  collection of lines, and a map which takes the prior line IDs to new line
  IDs."
  [lines]
  (->> lines
       (group-by (juxt :y0 :y1 :model))
       vals
       (reduce merge-lines-r [lines {}])))

(defn mean [xs]
  (assert (seq xs))
  (/ (reduce + 0 xs) (count xs)))

(defn clamp
  "Clamp x to lie within [min max]."
  [min- max- x]
  (cond (< x min-) min-
        (< max- x) max-
        true       x))

(defn choose-line-xs
  "Once we've merged lines and established their bounds, we have to actually
  *pick* some xs that satisfy those bounds--and the condition that we always
  propagate forwards in time.

  At this point in the solver, any x within [min-x, max-x) is legal, but we
  must:

  1. Pick coordinates so that x1 is greater than x0, and
  2. Pick *the same* coordinate for all predecessor/successor lines

  We'll start by choosing the least slope possible for every line. Then we'll
  gradually converge each line such that it aligns with its neighbors."
  ([lines]
   (assert (bounds-okay? lines))
   (let [lines  (->> lines
                     (mapv (fn [line]
                             (when line
                               (-> line
                                   (assoc :x0 (:min-x0 line))
                                   (assoc :x1 (:min-x1 line)))))))
;                     transient)
         n      (count lines)]
     (loop [lines       lines
            satisfied?  true
            i           0]
       (assert (bounds-okay? lines))
       (if (= i n)
         ; Done with this pass
         (do (prn :pass-----------------------)
         (if satisfied?
           lines
           ;(persistent! lines)
           (recur lines true 0)))

         ; Converge this line
         (let [{:keys [min-x0 max-x0 x0
                       min-x1 max-x1 x1] :as line} (get lines i)]
           (when line
             (assert (<= min-x0 x0 max-x0) line)
             (assert (<= min-x1 x1 max-x1) line))
           (cond
             ; Nothing here
             (nil? line) (recur lines satisfied? (inc i))

             ; We're okay
             (<= 0.05 (- x1 x0)) (recur lines satisfied? (inc i))

             ; This line's slope is too tight; push either start or finish out.
             true
             (if (or (< (rand) 0.5))
;                     (< (- max-x1 x1) (- x0 min-x0)))
               ; Adjust x0
               (let [_     (prn :too-tight [x0 x1] :in [min-x0 max-x0 min-x1 max-x1])
                     x0    (clamp min-x0 max-x0 (min (- x0 (rand))
                                                     (- x1 (rand))))
                     _     (prn :adjusted0 [x0 x1])
                     lines (propagate-x0 lines i x0)]
                 (recur lines false (inc i)))

               ; Adjust x1
               (let [_     (prn :too-tight [x0 x1] :in [min-x0 max-x0 min-x1 max-x1])
                     x1    (clamp min-x1 max-x1 (max (+ x1 (rand))
                                                     (+ x0 (rand))))
                     _     (prn :adjusted1 [x0 x1])
                     lines (propagate-x1 lines i x1)]
                 (recur lines false (inc i)))))))))))

(defn paths->lines
  "Many path components are degenerate--they refer to equivalent state
  transitions. We want to map a set of paths into a smaller set of lines from
  operation to operation. We have coordinate bounds on every transition. Our
  job is to find non-empty intersections of those bounds for equivalent
  transitions and collapse them.

  We compute two data structures:

  - A collection of paths, each transition augmented with a numeric :line-id
  which identifies the line leading to that transition.
  - An indexed collection of line IDs to {:x0, y0, :x1, :y1} line segments
  connecting transitions."
  [paths]
  (let [[paths lines]   (paths->initial-lines paths)
        _ (assert (bounds-okay? lines))
        [lines mapping] (merge-lines lines)
        _ (assert (bounds-okay? lines))
        lines           (choose-line-xs lines)
        _ (assert (bounds-okay? lines))
        mapping         (collapse-mapping mapping)
        paths           (map (fn [path]
                               (map (fn [transition]
                                      (assoc transition :line-id
                                             (mapping (:line-id transition))))
                                    path))
                               paths)]
    [paths lines]))

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
        time-coords     (time-coords pair-index time-bounds ops)
        paths           (paths analysis time-coords process-coords )
        [paths lines]   (paths->lines paths)]
    {:history       history
     :analysis      analysis
     :pair-index    pair-index
     :ops           ops
     :models        models
     :model-numbers model-numbers
     :process-coords process-coords
     :time-bounds   time-bounds
     :time-coords   time-coords
     :paths         paths
     :lines         lines}))

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
                (svg/group
                  (svg/rect (hscale t1)
                            (vscale p)
                            (vscale process-height)
                            (hscale width)
                            :rx (vscale 0.1)
                            :ry (vscale 0.1)
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

(defn render-lines
  "Given learnings, renders all lines as a group of SVG tags."
  [{:keys [lines]}]
  (->> lines
       (remove nil?)
       (mapv (fn [{:keys [id x0 y0 x1 y1] :as line}]
               (let [up? (< y0 y1)
                     y0  (if up? (+ y0 process-height) y0)
                     y1  (if up? y1 (+ y1 process-height))]
                 (prn :line line)
                 (svg/line (hscale x0) (vscale y0)
                           (hscale x1) (vscale y1)
                           :id            (str "line-" id)
                           :stroke-width  (vscale 0.025)
                           :stroke        (transition-color line)))))
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

(defn constrain-path
  "Applies constraints to a path, ensuring x coordinates are sequential and lie
  within the bounds of their parent operations."
  ([path]
   (constrain-path (transient (vec path)) (range (count path))))
  ([path indices]
   (let [[ok? path]
         (->> indices
              (reduce (fn [[ok? path] i]
                        (let [{:keys [min-x x max-x] :as t1} (nth path i)
                              prev-x (when (pos? i) (:x (nth path (dec i))))
                              next-x (when (< (inc i) (count path))
                                       (:x (nth path (inc i))))
                              x' (cond (<= x min-x)  (+ min-x 0.1)
                                       (<= max-x x)  (- max-x 0.1)
                                       (and prev-x (<= x prev-x)) (+ prev-x 0.1)
                                       (and next-x (<= next-x x)) (- next-x 0.1)
                                       true          x)]
                          [(and ok? (= x x'))
                           (assoc! path i (assoc t1 :x x'))]))
                      [true path]))]
    (if ok?
      (persistent! path)
      (recur path (shuffle indices))))))

(defn inverse-square-force
  "Computes force attracting x1 to x2 with strength c."
  [c x1 x2]
  (let [dist (- x2 x1)]
    (if (zero? dist)
      0
      (let [dir  (if (neg? dist) -1 1)
            scale (/ (Math/pow dist 2))]
        (* c dir scale)))))

(defn cap
  "Caps a delta to magnitude m"
  [m delta]
  (cond (< m delta)     m
        (< delta (- m)) (- m)
        true            delta))

(def relax-force   0.1)
(def cluster-force 0.000001)

(defn relax-path
  "Takes a single path and relaxes xs closer to their means."
  [strength path]
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
              t2 (nth path (inc i))
              mean (/ (+ (:x t0) (:x t2)) 2)
              x  (:x t1)
              x' mean]
          (recur (dec i)
                 (assoc! path i (assoc t1 :x x'))))))))

(defn cluster-op-transitions
  "Given a collection of transitions occurring on a single operation,
  identifies transitions with the same model and collapses their :x coordinate.
  Returns the new transitions in the same order.

  We want to establish clusters, but we don't necessarily know the scale or how
  many clusters should exist--and grouping to a single cluster will slap us up
  against the constraints. We'll take a gentler approach: apply an inverse
  quadratic force pushing apart different models and pulling together similar
  ones."
  [strength transitions]
  ; Compute centroids for each model
  (let [centroids (->> transitions
                       (group-by :model)
                       (map (fn [[model transitions]]
                              [model (/ (reduce + (map :x transitions))
                                        (count transitions))]))
                       (into {}))]
    (->> transitions
         (map (fn [transition]
                (assoc transition :x (centroids (:model transition))))))))

(defn cluster-transitions
  "Cluster transitions on each operation"
  [strength paths]
  ; This data's in the wrong shape. We have a collection of collections of
  ; transitions, but our clustering strategy works on individual transitions
  ; *across* paths. We have to get those transitions together, cluster them,
  ; then map the new transitions *back* into our path structure. We're not
  ; necessarily guaranteed that a given transition will be unique, so we can't
  ; just compute simple maps here.
  ;
  ; So, we'll assign a vector index to every transition: [path-index,
  ; transition-index]. Then we'll build a map of ops to indices. For each
  ; model, cluster the corresponding transitions, and assoc those back into the
  ; vector of paths.
  (let [; First we need a vector of vectors
        paths (mapv vec paths)

        ; Now a map of ops to collections of indices
        by-op (->> paths
                   (map-indexed (fn [i path]
                                  (map-indexed (fn [j transition]
                                                 {(:op transition) [[i j]]})
                                               path)))
                   (apply concat)
                   (apply merge-with into))]
    ; Now compute new transitions and update paths.
    (reduce (fn [paths' [op indices]]
              ; Compute new transitions
              (let [ts (->> indices
                            (map #(get-in paths %))
                            (cluster-op-transitions strength))]
                ; Merge transitions into paths
                (loop [indices indices
                       ts      ts
                       paths'  paths']
                  (if-not (seq indices)
                    paths'
                    (recur (next indices)
                           (next ts)
                           (assoc-in paths' (first indices) (first ts)))))))
            paths
            by-op)))

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
  (loop [i     1
         paths paths]
    (if (< 10 i)
      paths
      (let [cap (* 10 (/ (Math/pow i 2)))]
        (recur (inc i)
               (->> paths
                    (map (partial relax-path cap))
                    (map (partial relax-path cap))
                    (map (partial relax-path cap))
                    (map (partial relax-path cap))
                    (cluster-transitions cap)
                    (map constrain-path)))))))


(defn render-path
  "Renders a particular path, given learnings. Returns {:transitions, :models},
  each an SVG group. We render these separately because models go *on top* of
  ops, and transitions go *below* them."
  ([learnings path]
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
  (let [learnings  (learnings history analysis)
        ops        (render-ops learnings)
;        paths      (render-paths learnings)
        lines      (render-lines learnings)]
    (spit file
          (xml/emit
            (svg-2
              (-> (svg/group
                    lines
                    ops)
                  (svg/translate (vscale 1) (vscale 1))))))))
