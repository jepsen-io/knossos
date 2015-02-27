(ns knossos.op
  "Operations on operations!"
  (:require [clojure.core.typed :refer [
                                        ann
                                        check-ns
                                        defalias
                                        I
                                        IFn
                                        Keyword
                                        U
                                        Value
                                        ]]))

(defalias OpType
  "The possible types of operations."
  (U (Value :invoke)
     (Value :ok)
     (Value :fail)
     (Value :info)))

(defalias Op
  "An operation is comprised of a `process` invoking, completing, or noting
  progress of a function `f`, called with an argument `value`. Operations may
  also carry extra keys."
  (HMap :mandatory {:process  Object
                    :type     OpType
                    :f        Keyword
                    :value    Object}))

; Operation subtypes
(defalias Invoke (I Op (HMap :mandatory {:type (Value :invoke)})))
(defalias OK     (I Op (HMap :mandatory {:type (Value :ok)})))
(defalias Fail   (I Op (HMap :mandatory {:type (Value :fail)})))
(defalias Info   (I Op (HMap :mandatory {:type (Value :info)})))

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

(ann invoke? [Op -> Boolean])
(defn invoke? [op]
  "Is this op an invocation?"
  (= :invoke (:type op)))

(ann ok? [Op -> Boolean])
(defn ok? [op]
  "Is this op OK?"
  (= :ok     (:type op)))

(ann fail? [Op -> Boolean])
(defn fail? [op]
  "Is this op a failure?"
  (= :fail   (:type op)))

(ann same-process? [Op Op -> Boolean])
(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))
