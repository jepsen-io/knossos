(ns knossos.op
  "Operations on operations!")

(defn op
  "Constructs a new operation for a history."
  [process type f value]
  {:process process
   :type    type
   :f       f
   :value   value})

(defn invoke
  [process f value]
  (op process :invoke f value))

(defn ok
  [process f value]
  (op process :ok f value))

(defn fail
  [process f value]
  (op process :fail f value))

(defn info
  [process f value]
  (op process :info f value))

(defn invoke? [op] (= :invoke (:type op)))
(defn ok?     [op] (= :ok     (:type op)))
(defn fail?   [op] (= :fail   (:type op)))

(defn same-process?
  "Do A and B come from the same process?"
  [a b]
  (= (:process a)
     (:process b)))
