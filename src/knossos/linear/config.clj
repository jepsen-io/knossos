(ns knossos.linear.config
  "Datatypes for search configurations"
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [potemkin :refer [definterface+ deftype+ defrecord+]]
            [knossos [core :as core]
                     [util :refer :all]
                     [op :as op :refer [Op Invoke OK Info Fail]]])
    (:import knossos.core.Model
             java.util.Set
             java.util.HashSet))

;; An immutable map of process ids to whether they are calling or returning an
;; op, augmented with a mutable union-find memoized equality test.

(definterface+ Processes
  (calls      "A reducible of called but unlinearized operations."      [ps])

  (call       "Adds an operation being called with the calling state."  [ps op])
  (linearize  "Changes the given operation from calling to returning."  [ps op])
  (return     "Removes an operation from the returned set."             [ps op])

  (idle?      "Is this process doing nothing?"                          [ps p])
  (calling?   "Is this process currently calling an operation?"         [ps p])
  (returning? "Is this process returning an operation?"                 [ps p]))

; Maybe later
; (def ^:const idle      0)
; (def ^:const calling   1)
; (def ^:const returning 2)

;; A silly implementation based on two Clojure maps.
(defrecord MapProcesses [calls rets]
  Processes
  (calls [ps]
    (vals calls))

  (call [ps op]
    (let [p (:process op)]
      (assert (idle? ps p))
      (MapProcesses. (assoc calls p op) rets)))

  (linearize [ps op]
    (let [p (:process op)]
      (assert (calling? ps p))
      (MapProcesses. (dissoc calls p) (assoc rets p op))))

  (return [ps op]
    (let [p (:process op)]
      (assert (returning? ps p))
      (MapProcesses. calls (dissoc rets p))))

  (idle?      [ps p] (not (or (contains? calls p)
                              (contains? rets  p))))
  (calling?   [ps p] (contains? calls p))
  (returning? [ps p] (contains? rets p)))

(defn map-processes
  "A Processes tracker based on Clojure maps."
  []
  (MapProcesses. {} {}))

(deftype MemoizedMapProcesses [calls
                               rets
                               ^:volatile-mutable ^int hasheq
                               ^:volatile-mutable ^int hashcode]
  Processes
  (calls [ps]
    (vals calls))

  (call [ps op]
    (let [p (:process op)]
      (assert (idle? ps p))
      (MapProcesses. (assoc calls p op) rets)))

  (linearize [ps op]
    (let [p (:process op)]
      (assert (calling? ps p))
      (MapProcesses. (dissoc calls p) (assoc rets p op))))

  (return [ps op]
    (let [p (:process op)]
      (assert (returning? ps p))
      (MapProcesses. calls (dissoc rets p))))

  (idle?      [ps p] (not (or (contains? calls p)
                              (contains? rets  p))))
  (calling?   [ps p] (contains? calls p))
  (returning? [ps p] (contains? rets p))

  ; I'm assuming calls and rets will never be identical since we move ops
  ; atomically from one to the other, so we shouuuldn't see collisions here?
  ; Maye xor isn't good enough though. Might back off to murmur3.
  clojure.lang.IHashEq
  (hasheq [ps]
    (when (= -1 hasheq)
      (set! hasheq (int (bit-xor (hash calls) (hash rets)))))
    hasheq)

  Object
  (hashCode [ps]
    (when (= -1 hashcode)
      (set! hashcode (int (bit-xor (.hashCode calls) (.hashCode rets)))))
    hashcode)

  (equals [this other]
    (or (identical? this other)
        (and (instance? MemoizedMapProcesses other)
             (= (.hashCode this) (.hashCode other))
             (= calls (.calls other))
             (= rets  (.rets other))))))

(defn memoized-map-processes
  "A Processes tracker based on Clojure maps, with memoized hashcode for faster
  hashing and equality."
  []
  (MemoizedMapProcesses. {} {} -1 -1))


; One particular path through the history, comprised of a model and a tracker
; for process states.

(defrecord Config [model processes])

(defn config
  "An initial configuration around a given model."
  [model]
  (Config. model (map-processes)))

;; Non-threadsafe mutable configuration sets

(definterface+ ConfigSet
  (add! "Add a configuration to a config-set. Returns self.
        You do not need to preserve the return value."
        [config-set config]))

(deftype SetConfigSet [^:unsynchronized-mutable ^Set s]
  ConfigSet
  (add! [this config]
    (.add s config)
    this)

  clojure.lang.Counted
  (count [this] (.size s))

  clojure.lang.Seqable
  (seq [this] (seq s))

  Object
  (toString [this]
    (str "#{" (->> this
                   seq
                   (str/join #", "))
         "}")))

(defmethod print-method SetConfigSet [x ^java.io.Writer w]
  (.write w (str x)))

(defn set-config-set
  "An empty set-backed config set, or one backed by a collection."
  ([] (SetConfigSet. (HashSet.)))
  ([coll] (reduce add! (set-config-set) coll)))
