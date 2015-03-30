(ns knossos.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.typed :refer [check-ns]]
            [knossos.core :refer :all]
            [knossos.prioqueue :as prioqueue]
            [knossos.history :as history]
            [knossos.op :as op]
            [clojure.pprint :refer [pprint]])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)))

(deftest typecheck
  (is (check-ns 'knossos.core)))

(deftest apply-ops-test
  (let [ops [(op/ok :a :read 0)
             (op/ok :a :read nil)
             (op/ok :a :write 1)
             (op/ok :a :read 1)]]
    (is (= (-> (->Register 0)
               world
               (assoc :pending (set ops))
               (apply-ops (NonBlockingHashMapLong.) ops))
           (assoc (world (->Register 1)) :fixed ops)))

    (is (inconsistent-world?
                 (apply-ops (world (->Register 0))
                            (NonBlockingHashMapLong.)
                            [(op/ok :a :read 1)])))))


(deftest prioqueue-test
  (let [q  (prioqueue/prioqueue)
        w1 (assoc (world :good) :index 10 :pending #{1})
        w2 (assoc (world :evil) :index 20 :pending #{1 2 3 4 5 6})]
    (->> [w1 w2 w1 w2]
         (map #(prioqueue/put! q (awfulness %) %))
         (dorun))
    (is (= (take-while identity (repeatedly #(prioqueue/poll! q 1)))
           [w2 w2 w1 w1]))))

(defn valid-history
  [model history]
  (let [a (analysis model history)]
    (is (:valid? a))
    (when-not (:valid? a)
      (pprint history)
      (pprint a))))

(deftest register-no-ops
  (valid-history (register 0) []))

(deftest register-one-write-one-read
  (valid-history (register 0)
                 [{:process 1 :type :invoke :f :write :value 1}
                  {:process 1 :type :ok     :f :write :value 1}
                  {:process 2 :type :invoke :f :read  :value 1}
                  {:process 2 :type :ok     :f :read  :value 1}]))

(deftest register-concurrent-write-read
    (valid-history (register 0)
                   [{:process 1 :type :invoke :f :write :value 1}
                    {:process 2 :type :invoke :f :read  :value 1}
                    {:process 2 :type :ok     :f :read  :value 1}
                    {:process 1 :type :ok     :f :write :value 1}]))

(deftest history-a
  ; A CAS register history that exposed a particular knossos bug
  (valid-history (register 0)
                 [{:process 4, :type :invoke, :f :write, :value 2}
                  {:process 4, :type :ok, :f :write, :value 2}
                  {:process 6, :type :invoke, :f :read, :value nil}
                  {:process 1, :type :invoke, :f :write, :value 4}
                  {:process 1, :type :ok, :f :write, :value 4}
                  {:process 14, :type :invoke, :f :read, :value 4}
                  {:process 14, :type :ok, :f :read, :value 4}
                  {:process 9, :type :invoke, :f :write, :value 0}
                  {:process 9, :type :ok, :f :write, :value 0}
                  {:process 19, :type :invoke, :f :read, :value 0}
                  {:process 19, :type :ok, :f :read, :value 0}]))

(deftest nonlinearizable-concurrent-read-write-inconsistent-op
  ; An invalid register history which caused Knossos to incorrectly
  ; detect the invalid operation too early, instead of telling us correctly
  ; about the terminal worlds.
  (let [h [; Concurrent write of 1, and (impossible) read of 2.
           {:process 1, :type :invoke, :f :read}
           {:process 2, :type :invoke, :f :write, :value 1}
           {:process 1, :type :ok,     :f :read, :value 2}
           {:process 2, :type :ok,     :f :write, :value 1}]
        a (analysis (->Register 0) h)]
    (is (not (:valid? a)))
    (is (= (:inconsistent-op a)
           {:value 2, :process 1, :type :ok, :f :read}))
    (is (= (:causes a)
           [{:world (->World
                       (->Register 0)
                       []
                       #{{:value 2, :process 1, :type :invoke, :f :read}
                         {:value 1, :process 2, :type :invoke, :f :write}}
                       2)
             :op {:value 2, :process 1, :type :invoke, :f :read}
             :error "read 2 from register 0"}
            {:world (->World
                       (->Register 1)
                       [{:value 1, :process 2, :type :invoke, :f :write}]
                       #{{:value 2, :process 1, :type :invoke, :f :read}}
                       3)
             :op {:value 2, :process 1, :type :invoke, :f :read}
             :error "read 2 from register 1"}]))))

(defmacro dothreads
  "Calls body in each of t threads, blocking until completion. Binds i
  to the thread number."
  [[i t] & body]
  `(->> (range ~t)
        (map (fn [~i] (future ~@body)))
        doall
        (map deref)
        dorun))

(defprotocol Mutable
  (get-mutable [this])
  (set-mutable [this x]))

(deftype VolatileVariable [^:volatile-mutable v]
  Mutable
  (get-mutable [this] v)
  (set-mutable [this x] (set! v x)))

(defn volatile-history
  "Records a linearizable history by mucking around with a volatile
  mutable variable. Crash-factor is the likelihood that a process crashes
  during an operation: 0 is always reliable, 1 always crashes. Processes may
  crash before or after an operation actually occurs."
  [process-count action-count crash-factor]
  (let [history (atom [])
        x       (VolatileVariable. 0)]
    (dothreads [process process-count]
               (try
                 (dotimes [i action-count]
                   (Thread/sleep (rand-int 5))
                   (when (< (rand) crash-factor) (assert false))

                   (let [value (rand-int 10)]
                     (if (< (rand) 0.5)
                       ; Write
                       (do
                         (swap! history conj (op/invoke process :write value))
                         (set-mutable x value)
                         (when (< (rand) crash-factor) (assert false))
                         (swap! history conj (op/ok process :write value)))
                       ; Read
                       (do
                         (swap! history conj (op/invoke process :read nil))
                         (let [value (get-mutable x)]
                           (when (< (rand) crash-factor) (assert false))
                           (swap! history conj (op/ok process :read value)))))))
                 (catch AssertionError _ :crashed)))
    @history))

(deftest volatile-linearizable-test
  (dotimes [i 1]
    (let [history (volatile-history 100 100 1/10000)
          _       (prn (count history))
          a       (analysis (->Register 0) history)]
      (is (:valid? a))
      (when-not (:valid? a)
        (pprint history)
        (println "history length" (count history))
        (prn)
        (pprint (update-in a [:worlds] (partial take 10)))))))
