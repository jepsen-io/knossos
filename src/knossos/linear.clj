(ns knossos.linear
  "An implementation of the linear algorithm from
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf"
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn error]]
            [knossos.linear.config :as config]
            [knossos.model.memo :refer [memo]]
            [knossos [core :as core]
                     [history :as history]
                     [model :as model]
                     [util :refer :all]
                     [op :as op]])
  (:import [java.util ArrayList]))

;; Transitions between configurations

(defn t-call
  "If calls and rets contain no call for a process, that process can invoke an
  operation, generating a new configuration."
  [config op]
  (assoc config :processes (config/call (:processes config) op)))

(defn t-lin
  "A pending call can be linearized by transforming the current state, moving
  the invocation from calls to rets. Returns nil if linearizing this operation
  would be inconsistent."
  [config op]
  (let [model' (model/step (:model config) op)]
    (when-not (model/inconsistent? model')
      (config/->Config model'
                       (config/linearize (:processes config) op)))))

(defn t-ret
  "A pending return can be completed. It's legal to provide an invocation or a
  completion here--all that matters is the :process"
  [config op]
  (assoc config :processes (config/return (:processes config) op)))

;; Exploring the automaton

(defn jit-linearizations
  "Takes an initial configuration, a collection of pending invocations, and a
  final invocation to linearize and return. Explores all orders of pending
  invocations, and returns a sequence of valid configurations with that final
  invocation linearized."
  ([config final]
   (jit-linearizations final (ArrayList.) config))
;   (jit-linearizations final (config/set-config-set) config))
;   (persistent!
;     (jit-linearizations final (transient []) config)))
  ; Build up a transient vector of resulting configs recursively.
  ([final ^ArrayList configs config]
   ; Trivial case: record this configuration.
   (.add configs config)
   (let [configs        configs
         final-process  (int (:process final))]
     ; Take all pending ops from the configuration *except* the final one,
     ; try linearizing that op, and if we could linearize it, explore its
     ; successive linearizations too.
     (->> config
          :processes
          config/calls
          (r/filter (fn excluder [op]
                      (not (= final-process (int (:process op))))))
          (rkeep    (fn linearizer [op] (t-lin config op)))
          (reduce   (fn recurrence [configs op]
                      (jit-linearizations final configs op))
                  configs)))))

(defn step-ok!
  "Takes a ConfigSet, a config and an ok operation. If the operation has
  already been linearized, returns a ConfigSet containing only `configuration`,
  but with the ok op removed from the config's rets.

  If the operation hasn't been linearized yet, takes all pending calls from
  other threads and linearizes them in each possible order, creating a set of
  new configurations. Finally, attempts to linearize and return the given
  operation on each of those configs.

  Returns the ConfigSet."
  [config-set config ok]
  (let [p (:process ok)]
    (cond
      ; Have we already linearized the ok op in this config?
      (config/returning? (:processes config) p)
      (config/add! config-set (t-ret config ok))

      ; Is this operation pending?
      (config/calling? (:processes config) p)
      (let [jit-configs (jit-linearizations config ok)]
        ; Apply ok op to each one and return it.
        (->> jit-configs
             (rkeep (fn final-linearization [config]
                       (when-let [linearized (t-lin config ok)]
                         (t-ret linearized ok))))
             (reduce config/add! config-set))))

    config-set))

(def ^:const parallel-threshold
  "How many configs do we need before we start parallelizing?"
  128)

(defn step
  "Advance one step through the history. Takes a configset, returns a new
  configset--or a reduced failure."
  [state configs op]
  (reset! state {:running? true :configs configs :op op})
  (cond
    ; If we're invoking an operation, just add it to each config's pending ops.
    (and (op/invoke? op) (not (:fails? op)))
    (->> configs
         (map #(t-call % op))
         (reduce config/add! (config/set-config-set)))

    ; If we're handling a completion, run each configuration through a
    ; just-in-time linearization, accumulating a new set of configs.
    (op/ok? op)
    (let [configs' (if (< (count configs) parallel-threshold)
                     ; Singlethreaded search
                     (let [configs' (config/set-config-set)]
                       (doseq [c configs]
                         (step-ok! configs' c op))
                       configs')

                     ; Parallel search
                     (->> configs
                          (pmap (fn par-expand [config]
                                  (step-ok! (config/set-config-set) config op)))
                          (apply concat)
                          ; Merge parallel results
                          (reduce config/add! (config/set-config-set))))]

      (if (empty? configs')
        ; Out of options! Return a reduced debugging state.
        (reduced {:valid?  false
                  :configs (map config/config->map configs)
                  :op      op})
        ; Otherwise, return new config set.
        configs'))

    ; Skip other types of ops
    true
    configs))

(def reporting-interval
  "How long (in ms) to sleep between status updates"
  5000)

(defn reporter!
  "Spawns a reporting thread that periodically logs the state of the analysis.
  State is an atom with :running?, :configs, and :op."
  [state]
  (future
    (with-thread-name "reporter"
      (loop [last-state nil]
        (when (:running? @state)
          (Thread/sleep reporting-interval)
          (let [state @state]
            (if (not= state last-state)
              (do (info :space (count (:configs state))
                        :op    (:op state))
                  (recur state))
              (recur last-state))))))))

(defn analysis
  "Given an initial model state and a history, checks to see if the history is
  linearizable. Returns a map with a :valid? bool and a :config configset from
  its final state, plus an offending :op if an operation forced the analyzer to
  discover there are no linearizable paths."
  [model history]
  (let [history (-> history
                    history/complete
                    history/index)
        memo (memo model history)
        history (:history memo)
        configs (-> (:model memo)
                    (config/config history)
                    list
                    config/set-config-set)
        state (atom {:running?  true
                     :configs   configs
                     :op        (first history)})
        reporter (reporter! state)
        res (reduce (partial step state) configs history)]
    (if (and (map? res) (= false (:valid? res)))
      ; Reduced error
      res
      {:valid?  true
       :configs (map config/config->map res)})))
