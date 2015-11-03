(ns knossos.extra-types
  "Typehints for things outside our ken"
  (:require [clojure.core.typed :as t
             :refer [
                     All
                     Any
                     ann
                     ann-form
                     ann-record
                     AnyInteger
                     Atom1
                     Atom2
                     defalias
                     HVec
                     I
                     IFn
                     inst
                     NonEmptyVec
                     Option
                     Set
                     Seqable
                     tc-ignore
                     U
                     Vec
                     ]]
            [interval-metrics.core :as metrics]
            [clojure.core.reducers :as r]
            [clojure.math.combinatorics :as combo]
            [clojure.tools.logging :as log])
  (:import (clojure.tools.logging.impl Logger
                                       LoggerFactory)
           (interval_metrics.core Metric)
           (clojure.lang IMapEntry
                         IPersistentMap
                         IPersistentSet
                         IPersistentVector
                         IPersistentCollection
                         ITransientMap
                         ITransientSet
                         ITransientVector
                         ITransientCollection)))

; loltransients
(ann clojure.core/transient
     (IFn [(IPersistentMap Any Any)     -> ITransientMap]
          [(IPersistentSet Any)         -> ITransientSet]
          [(IPersistentVector Any)      -> ITransientVector]
          [(IPersistentCollection Any)  -> ITransientCollection]))

(ann clojure.core/persistent!
     (IFn [ITransientMap -> IPersistentMap]
          [ITransientSet -> IPersistentSet]
          [ITransientVector -> IPersistentVector]
          [ITransientCollection -> IPersistentCollection]))

(ann clojure.core/conj!
     (IFn [ITransientMap (U IMapEntry (HVec [Any Any])) -> ITransientMap]
          [ITransientSet        Any -> ITransientSet]
          [ITransientVector     Any -> ITransientVector]
          [ITransientCollection Any -> ITransientCollection]))

(ann clojure.core/assoc!
     (IFn [ITransientMap    Any Any        -> ITransientMap]
          [ITransientVector AnyInteger Any -> ITransientVector]))

(ann clojure.core/dissoc! [ITransientMap Any -> ITransientMap])


; Pretend reducibles are seqable though they totally aren't. Hopefully
; core.typed will have reducible tfns and annotations for reduce et al someday
(ann ^:no-check clojure.core.reducers/map
     (All [a b] (IFn [[a -> b] (Seqable a) -> (Seqable b)])))

(ann ^:no-check clojure.core.reducers/remove
     (All [a b]
          (IFn [[a -> Any :filters {:then tt, :else (is b 0)}] (Seqable a)
                -> (Seqable b)]
               [[a -> Any :filters {:then tt, :else (! b 0)}] (Seqable a)
                -> (Seqable (I a (Not b)))]
               [[a -> Any] (Seqable a) -> (Seqable a)])))

(ann ^:no-check clojure.core.reducers/mapcat
     (All [a b] (IFn [[a -> (Seqable b)] (Seqable a) -> (Seqable b)])))

(ann ^:no-check clojure.core.reducers/foldcat
     (All [a] (IFn [(Seqable a) -> (Seqable a)])))

; math.combinatorics
(ann ^:no-check clojure.math.combinatorics/permutations
     (All [a] (IFn [(Seqable a) -> (Seqable (Seqable a))])))

(ann ^:no-check clojure.math.combinatorics/subsets
     (All [a] (IFn [(Seqable a) -> (Seqable (Seqable a))])))

; interval-metrics
(ann ^:no-check interval-metrics.core/update!   [Metric Any -> Metric])
(ann ^:no-check interval-metrics.core/snapshot! [Metric -> Any])

; tools.logging
(ann ^:no-check clojure.tools.logging.impl/enabled? [Logger Any -> Boolean])
(ann ^:no-check clojure.tools.logging.impl/get-logger [Any Any -> Logger])
(ann ^:no-check clojure.tools.logging/*logger-factory* LoggerFactory)
(ann ^:no-check clojure.tools.logging/log*
     [Logger Any (Option Throwable) Any -> (Value nil)])

