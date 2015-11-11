(ns knossos.linear.config
  "Datatypes for search configurations"
  (:require [potemkin :refer [definterface+]]
            [knossos [core :as core]
             [op :as op :refer [Op Invoke OK Info Fail]]])
    (:import knossos.core.Model
             java.util.Set
             java.util.HashSet))

; One particular path through the history, comprised of a model, set of
; outstanding non-linearized calls, and outstanding linearized returns. We
; represent invocations, linearizations, and completions by invokes. Calls and
; returns are a map of process ID to invocations.
(defrecord Config [model calls rets])

(defn config
  "An initial configuration around a given model."
  [model]
  (Config. model {} {}))

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
  (seq [this] (seq s)))

(defn set-config-set
  "An empty set-backed config set, or one backed by a collection."
  ([] (SetConfigSet. (HashSet.)))
  ([coll] (reduce add! (set-config-set) coll)))
