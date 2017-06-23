(ns knossos.core-test
  (:require [clojure.test :refer :all]
            [knossos.core :refer :all]
            [knossos.prioqueue :as prioqueue]
            [knossos.history :as history]
            [knossos.model :as model :refer [register]]
            [knossos.op :as op]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (java.io PushbackReader)))

(defn read-history
    "Reads a history file of [process type f value] tuples, or maps."
    [f]
    (with-open [r (PushbackReader. (io/reader f))]
      (->> (repeatedly #(edn/read {:eof nil} r))
           (take-while identity)
           (map (fn [op]
                  (if (map? op)
                    op
                    (let [[process type f value] op]
                      {:process process
                       :type    type
                       :f       f
                       :value   value}))))
           vec)))

(defn read-history-2
  "Reads an EDN history file containing a single collection of operation maps."
  [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (edn/read r)))

(defn write-history!
  "Write a history to a file"
  [file history]
  (with-open [w (io/writer file)]
    (binding [*out* w]
      (pprint history))))

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
                 (catch AssertionError _
                   (swap! history conj (op/info process :crash nil))
                   :crashed)))
    @history))

(deftest prioqueue-test
  (let [q  (prioqueue/prioqueue)
        w1 [1 2]
        w2 [3 4 5]]
    (->> [w1 w2 w1 w2]
         (map #(prioqueue/put! q count %))
         (dorun))
    (is (= (take-while identity (repeatedly #(prioqueue/poll! q 1)))
           [w1 w1 w2 w2]))))

(defn analyze-file
  "Given a model m and a function that takes a model and a history and returns
  an analysis, runs that against a file. Confirms that the valid? field is
  equal to expected."
  [analyzer model expected file]
  (when (re-find #"\.edn$" (.getName file))
    (testing (.getName file)
      (let [h (read-history-2 file)
            a (analyzer model h)]
        (is (= expected (:valid? a)))
        a))))

(defn test-examples
  "Given a function that takes a model and a history and returns an analysis,
  runs that against known histories from data/bad and data/good, ensuring they
  produce correct analyses."
  [analyzer]
  (doseq [[model-name model]
          {:cas-register   (model/cas-register 0)
           :multi-register (model/multi-register {:x 0 :y 0})}]
    (doseq [valid? [true false]]
      (doseq [f (file-seq (io/file (str "data/" (name model-name)
                                       "/" (if valid? "good" "bad"))))]
        (println (.getCanonicalPath f))
        (analyze-file analyzer model valid? f)))))
