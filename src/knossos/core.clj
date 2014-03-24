(ns knossos.core
  (:require [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [potemkin :refer [definterface+]]))

(defn foldset
  "Folds a reducible collection into a set."
  [coll]
  (r/fold (r/monoid set/union hash-set)
          conj
          coll))

(defn op
  "Constructs a new operation for a history."
  [process type f value]
  {:process process
   :type    type
   :f       f
   :value   value})

(defn invoke-op
  [process f value]
  (op process :invoke f value))

(defn ok-op
  [process f value]
  (op process :ok f value))

(defn fail-op
  [process f value]
  (op process :fail f value))

(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))

(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model."))

(defrecord Inconsistent [msg]
  Model
  (step [this op] this))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

; A read-write register
(defrecord Register [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (Register. (:value op))
      :read  (if (or (nil? (:value op))     ; We don't know what the read was
                     (= value (:value op))) ; Read was a specific value
               r
               (inconsistent
                 (str "read " (pr-str (:value op))
                      " from register " value))))))

(defn complete
  "When a request is initiated, we may not know what the result will be--but
  find out when it completes. In the history, this might look like

  [{:type :invoke
    :f    :read
    :value nil}    ; We don't know what we're going to read.
   {:type  :ok
    :f     :read
    :value 2}]     ; We received 2.

  This function fills in missing values for invocations, where those requests
  complete. It constructs a new history in which we 'already knew' what the
  results of successful operations would have been.

  For failed operations, complete fills in the value for both invocation
  and completion; depending on whichever has a value available."
  [history]
  (->> history
       (reduce
         (fn complete [[history index] op]
           ; History is our complete history of operations. Index is a map of
           ; processes to the index of their most recent invocation. Note that
           ; we assume processes are singlethreaded; e.g. they do not perform
           ; multiple invocations without receiving responses.
           (condp = (:type op)
             ; An invocation; remember where it is
             :invoke
             (let [i (count history)]
               ; Enforce the singlethreaded constraint.
               (when-let [prior (get index (:process op))]
                 (throw (RuntimeException.
                          (str "Process " (:process op) " already running "
                               (pr-str (get history prior))
                               ", yet attempted to invoke "
                               (pr-str op) " concurrently"))))

               [(conj! history op)
                (assoc! index (:process op) i)])

             ; A completion; fill in the completed value.
             :ok
             (let [i           (get index (:process op))
                   _           (assert i)
                   invocation  (nth history i)
                   value       (or (:value invocation) (:value op))]
               [(-> history
                    (assoc! i (assoc invocation :value value))
                    (conj!  op))
                (dissoc! index (:process op))])

             ; A failure; fill in either value.
             :fail
             (let [i           (get index (:process op))
                   _           (assert i)
                   invocation  (nth history i)
                   value       (or (:value invocation) (:value op))]
               [(-> history
                    (assoc! i (assoc invocation :value value))
                    (conj!    (assoc op :value value)))
                (dissoc! index (:process op))])

             ; No change for info messages
             :info
             [(conj! history op) index]))
         [(transient []) (transient {})])
       first
       persistent!))

(defn world
  "A world represents the state of the system at one particular point in time.
  It comprises a known timeline of operations, and a set of operations which
  are pending."
  [model]
  {:model   model
   :fixed []
   :pending #{}})

(defn inconsistent-world?
  "Is the model for this world in an inconsistent state?"
  [world]
  (instance? Inconsistent (:model world)))

(defn advance-world
  "Given a world and a series of operations, applies those operations to the
  given world. The returned world will have a new model reflecting its state
  with the given operations applied, and its fixed history will have the given
  history appended. Those ops will also be absent from its pending operations."
  [world ops]
; (prn "advancing" world "with" ops)
  (-> world
      transient
      (assoc! :fixed   (concat (:fixed world) ops))
      (assoc! :model   (reduce step (:model world) ops))
      (assoc! :pending (persistent!
                         (reduce disj! (transient (:pending world)) ops)))
      persistent!))

(defn possible-worlds
  "Given a world, generates all possible future worlds consistent with the
  given world's pending operations. For instance, in the world

  {:fixed [:a :b]
   :pending [:c :d]}

  We *know* :a and :b have already happened, but :c and :d *could* happen, in
  either order, so long as they occur after :a and :b. Here are the possible
  worlds:

  None of them may have happened:

  {:fixed [:a :b]
   :pending [:c :d]}

  One of the two could have happened:

  {:fixed [:a :b :c]
   :pending [:d]}
  {:fixed [:a :b :d]
   :pending [:c]}

  Both could have happened:

  {:fixed [:a :b :c :d]
   :pending []}
  {:fixed [:a :b :d :c]
   :pending []}"
  [world]
  (let [worlds (->> world
                    :pending
                    combo/subsets                 ; oh no
                    (r/mapcat combo/permutations) ; dear lord no
                    ; For each permutation, advance the world with those
                    ; operations in order
                    (r/map (fn advance [ops] (advance-world world ops)))
                    ; Filter out null worlds
                    (r/remove nil?)
                    ; Realize
                    r/foldcat)

        ; Filter out inconsistent worlds
        consistent (->> worlds
                        (r/remove inconsistent-world?)
                        r/foldcat)]
    (cond
      ; No worlds at all
      (empty? worlds) worlds

      ; All worlds were inconsistent
      (empty? consistent) (throw (RuntimeException.
                                   (:msg (:model (first worlds)))))

      ; Return consistent worlds
      true consistent)))

(defn fold-invocation-into-worlds
  "Given a sequence of worlds and a new invoke operation, adds this operation
  to the pending set for each, and projecting out a set of all possible worlds
  from those."
  [worlds invocation]
  (->> worlds
       (r/map (fn pend [world]
                (assoc world :pending (conj (:pending world) invocation))))
       (r/mapcat possible-worlds)))

(defn fold-completion-into-worlds
  "Given a sequence of worlds and a completion operation, returns only those
  worlds where that operation took place; e.g. is not still pending.

  TODO: replace the corresponding element in the history with the completion."
  [worlds completion]
  (let [process (:process completion)]
    (->> worlds
         (r/remove (fn [world]
                     (some #(= process (:process %)) (:pending world)))))))

(defn fold-failure-into-worlds
  "Given a sequence of worlds and a failed operation, returns only those worlds
  where that operation did not take place, and removes the operation from
  the pending ops in those worlds.

  Note that a failed operation is an operation which is *known* to have failed;
  e.g. the system *guarantees* that it did not take place. This is different
  from an *indeterminate* failure."
  [worlds failure]
  (let [process (:process failure)]
    (->> worlds
         (r/map (fn [world]
                  (let [pending (:pending world)]
                    (when-let [inv (some #(when (= process (:process %)) %)
                                         pending)]
                      ; In this world, we have not yet applied this operation
                      (assoc world :pending (disj pending inv))))))
         (r/remove nil?))))

(defn fold-op-into-worlds
  "Given a set of worlds and any type of operation, folds that operation into
  the set and returns a new set of possible worlds."
  [worlds op]
  (foldset
    (condp = (:type op)
      :invoke (fold-invocation-into-worlds worlds op)
      :ok     (fold-completion-into-worlds worlds op)
      :fail   (fold-failure-into-worlds    worlds op)
      :info   worlds)))

(defn linearizations
  "Given a model and a history, returns all possible worlds where that history
  is linearizable."
  [model history]
  (reduce fold-op-into-worlds
          (list (world model))
          history))

(defn linearizable-prefix
  "Computes the longest prefix of a history which is linearizable."
  [model history]
  (->> history
       (reduce (fn [[linearizable worlds] op]
                 (let [worlds' (fold-op-into-worlds worlds op)]
                   (prn :world-size (count worlds'))
                   (if (empty? worlds')
                     ; Out of options
                     (reduced [linearizable worlds])
                     [(conj linearizable op) worlds'])))
               [[] (list (world model))])
       first))
