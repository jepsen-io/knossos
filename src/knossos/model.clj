(ns knossos.model
  "A model specifies the behavior of a singlethreaded datatype, transitioning
  from one state to another given an operation."
  (:refer-clojure :exclude [set])
  (:import [clojure.lang PersistentQueue])
  (:require [clojure.set :as set]
            [knossos.op :as op]
            [potemkin :refer [definterface+]]
            [knossos.history :as history]
            [multiset.core :as multiset]))

(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model.

        Models should be a pure, deterministic function of their state and an
        operation's :f and :value."))

(defrecord Inconsistent [msg]
  Model
  (step [this op] this)

  Object
  (toString [this] msg))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

(defrecord NoOp []
  Model
  (step [m op] m))

(def noop
  "A model which always returns itself, unchanged."
  (NoOp.))

(defrecord Register [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (Register. (:value op))
      :read  (if (or (nil? (:value op))     ; We don't know what the read was
                     (= value (:value op))) ; Read was a specific value
               r
               (inconsistent
                 (str (pr-str value) "≠" (pr-str (:value op)))))))

  Object
  (toString [r] (pr-str value)))

(defn register
  "A read-write register."
  ([] (Register. nil))
  ([x] (Register. x)))

(defrecord CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str "can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str "can't read " (:value op)
                                  " from register " value)))))
  Object
  (toString [this] (pr-str value)))

(defn cas-register
  "A compare-and-set register"
  ([]      (CASRegister. nil))
  ([value] (CASRegister. value)))

(defrecord CausalRegister [value counter last-pos]
  Model
  (step [r op]
    (let [c (inc counter)
          v'   (:value op)
          pos  (:position op)
          link (:link op)]
      (if-not (or (= link :init)
                  (= link last-pos))
          (inconsistent (str "Cannot link " link
                             " to last-seen position " last-pos))
          (condp = (:f op)
            :write (cond
                     ;; Write aligns with next counter, OK
                     (= v' c)
                     (CausalRegister. v' c pos)

                     ;; Attempting to write an unknown value
                     (not= v' c)
                     (inconsistent (str "expected value " c
                                        " attempting to write "
                                        v' " instead")))

            :read-init  (cond
                          ;; Read a non-0 value from a freshly initialized register
                          (and (=    0 counter)
                               (not= 0 v'))
                          (inconsistent (str "expected init value 0, read " v'))

                          ;; Read the expected value of the register,
                          ;; update the last known position
                          (or (nil? v')
                              (= value v'))
                          (CausalRegister. value counter pos)

                          ;; Read a value that we haven't written
                          true (inconsistent (str "can't read " v'
                                                  " from register " value)))

            :read  (cond
                     ;; Read the expected value of the register,
                     ;; update the last known position
                     (or (nil? v')
                         (= value v'))
                     (CausalRegister. value counter pos)

                     ;; Read a value that we haven't written
                     true (inconsistent (str "can't read " v'
                                             " from register " value)))))))
  Object
  (toString [this] (pr-str value)))

(defn causal-register []
  (CausalRegister. 0 0 nil))

(defrecord Mutex [locked?]
  Model
  (step [r op]
    (condp = (:f op)
      :acquire (if locked?
                 (inconsistent "already held")
                 (Mutex. true))
      :release (if locked?
                 (Mutex. false)
                 (inconsistent "not held"))))

  Object
  (toString [this] (if locked? "locked" "free")))

(defn mutex
  "A single mutex responding to :acquire and :release messages"
  []
  (Mutex. false))

(defrecord MultiRegister []
  Model
  (step [this op]
    (assert (= (:f op) :txn))
    (reduce (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :read  (if (or (nil? v)
                               (= v (get state k)))
                         state
                         (reduced
                           (inconsistent
                             (str (pr-str (get state k)) "≠" (pr-str v)))))
                :write (assoc state k v)))
            this
            (:value op))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))

(defrecord Set [s]
  Model
  (step [this op]
    (condp = (:f op)
      :add (Set. (conj s (:value op)))
      :read (if (= s (:value op))
              this
              (inconsistent (str "can't read " (pr-str (:value op)) " from "
                                 (pr-str s)))))))

(defn set
  "A set which responds to :add and :read."
  []
  (Set. #{}))

(defrecord UnorderedQueue [pending]
  Model
  (step [r op]
    (condp = (:f op)
      :enqueue (UnorderedQueue. (conj pending (:value op)))
      :dequeue (if (contains? pending (:value op))
                 (UnorderedQueue. (disj pending (:value op)))
                 (inconsistent (str "can't dequeue " (:value op)))))))

(defn unordered-queue
  "A queue which does not order its pending elements."
  []
  (UnorderedQueue. (multiset/multiset)))

(defrecord FIFOQueue [pending]
  Model
  (step [r op]
    (condp = (:f op)
      :enqueue (FIFOQueue. (conj pending (:value op)))
      :dequeue (cond (zero? (count pending))
                     (inconsistent (str "can't dequeue " (:value op)
                                        " from empty queue"))

                     (= (:value op) (peek pending))
                     (FIFOQueue. (pop pending))

                     :else
                     (inconsistent (str "can't dequeue " (:value op)))))))

(defn fifo-queue
  "A FIFO queue."
  []
  (FIFOQueue. PersistentQueue/EMPTY))
