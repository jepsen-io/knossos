(ns knossos.linear
  "An implementation of the linear algorithm from
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf"
  (:refer-clojure :exclude [defn fn])
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.core.typed :as t
                                :refer [
                                        All
                                        Any
                                        ann
                                        ann-form
                                        ann-record
                                        Atom1
                                        Atom2
                                        defalias
                                        defn
                                        fn
                                        I
                                        IFn
                                        inst
                                        Map
                                        NonEmptyVec
                                        Option
                                        Set
                                        Seqable
                                        tc-ignore
                                        U
                                        Vec
                                        ]]
            [knossos [core :as core :refer [Model]]
                     [op :as op :refer [Op Invoke OK Info Fail]]]))

(ann-record Configuration
; "One particular path through the history, comprised of a model, set of
;  outstanding non-linearized calls, and outstanding linearized returns. Calls
;  and returns are a map of process ID to invocation or model, respectively."
  [:model :- Model
   :calls :- (Map Object Invoke)
   :rets  :- (Map Object Model)])

(defrecord Configuration [model calls rets])

(defn initial-configuration
  "Configuration used to start exploring the system."
  [model :- Model] :- Configuration
  (Configuration. model {} {}))

; Transitions between configurations
(defn t-call
  "If calls and rets contain no call for process p, p can invoke an operation."
  [config :- Configuration, op :- Invoke] :- Configuration
  (let [p (:process op)]
    (assert (not (contains? (:calls config) p)))
    (assert (not (contains? (:rets config) p)))
    (assoc config :calls (assoc (:calls config) p op))))

(defn t-lin
  "A pending call can be linearized by transforming the current state. Returns
  nil if the call could not be linearized."
  [config :- Configuration, op :- Invoke] :- (Option Configuration)
  (let [p (:process op)]
    (assert (contains? (:calls config) p))
    (assert (= op (get (:calls config) p)))
    (let [model' (core/invoke (:model config) op)]
      (when-not (core/inconsistent? model')
        (Configuration. model'
                        (dissoc (:calls config) p)
                        (assoc (:rets config) p model'))))))

(defn t-ret
  "A pending return can be completed"
  [config :- Configuration, process :- Object] :- Configuration
  (assert (contains? (:rets config process)))
  (assoc config :rets (dissoc (:rets config) process)))

(defalias Configurations (Seqable Configuration))

(defn step-ok
  "Takes a config and an ok operation. Takes all pending calls from other
  threads and linearizes them in each possible order, creating a set of new
  configurations. Finally, attempts to linearize and return the given operation
  on each of those configs."
  [config :- Configuration, ok :- OK]
  (let [; Take calls from other threads
        calls (-> config :calls (dissoc (:process ok)) vals)
        ; Compute all orders
        orders (-> calls combo/subsets combo/permutations)
        ; Consistent worlds resulting from linearizing those operations
        configs (->> orders
                     (map (fn [order :- (Seqable Invoke)
                               (keep (partial t-lin config))
                     


(defn step
  "Advance one step through the history."
  [configs :- Configurations, op :- Op] :- Configurations
  (let [p (:process op)]
    (cond
      ; Add the invocation to every configuration
      (op/invoke? op)
      (mapv (fn [c :- Config] (t-call config op)) configs)

      ; For oks, take all sequneces of pending calls of other threads
