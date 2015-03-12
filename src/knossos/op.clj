(ns knossos.op
  "Operations on operations!"
  (:require [clojure.core.typed :refer [
                                        ann
                                        check-ns
                                        defalias
                                        I
                                        IFn
                                        Keyword
                                        TFn
                                        U
                                        Value
                                        ]]))

; Operations come in four flavors
(defalias GenOp (TFn [[type :variance :covariant]]
                     (HMap :mandatory {:process Object
                                       :type    type
                                       :f       Keyword
                                       :value   Object})))

(defalias Invoke (GenOp (Value :invoke)))
(defalias OK     (GenOp (Value :ok)))
(defalias Fail   (GenOp (Value :fail)))
(defalias Info   (GenOp (Value :info)))

(defalias Op
  "An operation is comprised of a `process` invoking, completing, or noting
  progress of a function `f`, called with an argument `value`. Operations may
  also carry extra keys."
  (U Invoke OK Fail Info))

(ann op (IFn [Object (Value :invoke) Keyword Object -> Invoke]
             [Object (Value :ok)     Keyword Object -> OK]
             [Object (Value :fail)   Keyword Object -> Fail]
             [Object (Value :info)   Keyword Object -> Info]))
(defn op
  "Constructs a new operation for a history."
  [process type f value]
  {:process process
   :type    type
   :f       f
   :value   value})

(ann invoke [Object Keyword Object -> Invoke])
(defn invoke
  "Constructs an invocation op."
  [process f value]
  (op process :invoke f value))

(ann ok [Object Keyword Object -> Op])
(defn ok
  "Constructs an OK op."
  [process f value]
  (op process :ok f value))

(ann fail [Object Keyword Object -> Op])
(defn fail
  "Constructs a fail op."
  [process f value]
  (op process :fail f value))

(ann info [Object Keyword Object -> Op])
(defn info
  "Constructs an info op."
  [process f value]
  (op process :info f value))

; Not sure how to convince core.typed that this restricts Op to OK; c.t sees
; that it has (Key :type) = :ok, but can't seem to unify that with the Op
; constraint.
(ann ^:no-check ok? [Op -> Boolean :filters {:then (is OK 0)
                                             :else (!  OK 0)}])
(defn ok?
  "Is this op OK?"
  [op]
  (= :ok (:type op)))

(ann ^:no-check invoke? [Op -> Boolean :filters {:then (is Invoke 0)
                                                 :else (!  Invoke 0)}])
(defn invoke?
  "Is this op an invocation?"
  [op]
  (= :invoke (:type op)))

(ann ^:no-check fail? [Op -> Boolean :filters {:then (is Fail 0)
                                               :else (!  Fail 0)}])
(defn fail?
  "Is this op a failure?"
  [op]
  (= :fail (:type op)))

(ann ^:no-check info? [Op -> Boolean :filters {:then (is Info 0)
                                               :else (!  Info 0)}])
(defn info?
  "Is this op an informational message?"
  [op]
  (= :info (:type op)))


(ann same-process? [Op Op -> Boolean])
(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))
