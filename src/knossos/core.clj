(ns knossos.core
  (:require [knossos.op :as op]
            [knossos.competition :as competition]))

; For backwards compatibility

(def op op/op)

(def invoke-op  op/invoke)
(def ok-op      op/ok)
(def fail-op    op/fail)

(def invoke? op/invoke?)
(def ok?     op/ok?)
(def fail?   op/fail?)

(defn analysis
  "Alias for competition/analysis"
  ([model history]
   (analysis model history {}))
  ([model history opts]
   (competition/analysis model history opts)))
