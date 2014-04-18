(ns knossos.core-test
  (:require [clojure.test :refer :all]
            [knossos.core :refer :all]
            [knossos.prioqueue :as prioqueue]
            [knossos.history :as history]
            [knossos.op :as op]
            [clojure.pprint :refer [pprint]]))

(comment
  (deftest keep-singular-test
    (testing "Empty"
      (is (= (keep-singular inc []))))

    (testing "All OK"
      (is (= (keep-singular inc [1 2 3]) [2 3 4])))

    (testing "One bad at the start"
      (is (= (keep-singular inc [nil 1 2] [1 2]))))

    (testing "One bad at the end"
      (is (= (keep-singular inc [1 2 nil] [1 2]))))

    (testing "All bad"
      (is (thrown? java.lang.ClassCastException
                   (doall (keep-singular inc ["a" "b"])))))))

(deftest advance-world-test
  (let [ops [(op/ok :a :read 0)
             (op/ok :a :read nil)
             (op/ok :a :write 1)
             (op/ok :a :read 1)]]
    (is (= (-> (->Register 0)
               world
               (assoc :pending (set ops))
               (advance-world ops))
           (assoc (world (->Register 1)) :fixed ops)))

    (is (inconsistent-world?
                 (advance-world (world (->Register 0))
                                [(op/ok :a :read 1)])))))

(deftest possible-worlds-test
  (testing "empty"
    ; The projection of an empty world is an empty world. I'm so alone.
    (is (= (possible-worlds (world (->Register 1)))
           [(world (->Register 1))])))

  (testing "fixed history"
    (let [w (-> (world (->Register 0))
                (assoc :fixed [(op/ok :a :read 0)]))]
      (is (= (possible-worlds w) [w]))))

  (testing "one fixed, one pending operation"
    (let [w (-> (world (->Register 0))
                (assoc :fixed    [(op/ok :a :read 0)])
                (assoc :pending #{(op/ok :a :write 1)}))]
      (is (= (set (possible-worlds w))
             ; In one world, the write occurs
             #{(-> (world (->Register 1))
                   (assoc :fixed [(op/ok :a :read 0)
                                  (op/ok :a :write 1)])
                   (assoc :pending #{}))
               ; And in the other, the write is still pending.
               w}))))

  (testing "Multiple pending operations with only two linear paths"
    (let [w (-> (world (->Register 0))
                (assoc :pending #{(op/ok :a :read 0)
                                  (op/ok :a :write 1)
                                  (op/ok :a :read 1)}))]
      (is (= (set (possible-worlds w))
             #{; Nothing happened
               w
               ; Read 0 happened
               (-> w
                   (assoc :fixed   [(op/ok :a :read 0)])
                   (assoc :pending #{(op/ok :a :write 1)
                                     (op/ok :a :read 1)}))
               ; Read 0 and write 1 happened.
               (-> w
                   (assoc :model   (->Register 1))
                   (assoc :fixed   [(op/ok :a :read 0)
                                    (op/ok :a :write 1)])
                   (assoc :pending #{(op/ok :a :read 1)}))
               ; All ops happened
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(op/ok :a :read 0)
                                  (op/ok :a :write 1)
                                  (op/ok :a :read 1)])
                   (assoc :pending #{}))
               ; Write 1 happened.
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(op/ok :a :write 1)])
                   (assoc :pending #{(op/ok :a :read 0)
                                     (op/ok :a :read 1)}))
               ; Write 1 and read 1 happened.
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(op/ok :a :write 1)
                                  (op/ok :a :read 1)])
                   (assoc :pending #{(op/ok :a :read 0)}))})))))

(deftest linearizations-test
  (testing "Empty history"
    (is (= (set (linearizations (->Register 0) []))
           #{(world (->Register 0))})))

  (testing "Single invocation and completion."
    (is (= (set (linearizations (->Register 0)
                                [(op/invoke :a :read 0)
                                 (op/ok :a :read 0)]))
           #{(-> (world (->Register 0))
                 (assoc :index 2
                        :fixed [(op/invoke :a :read 0)]))})))

  (testing "Single invocation and failure."
    (is (= (set (linearizations (->Register 0)
                                [(op/invoke :a :read 0)
                                 (op/fail   :a :read 0)]))
           #{(-> (world (->Register 0))
                 (assoc :index 2))})))

  (testing "Simple read-write race with one linearization."
    (is (= (set (linearizations (->Register 0)
                                [(op/invoke :a :read 0)
                                 (op/ok :a :read 0)
                                 (op/invoke :a :read 1)   ; Overlaps B's write
                                 (op/invoke :b :write 1)
                                 (op/ok :a :read 1)
                                 (op/ok :b :write 1)]))
           #{(-> (world (->Register 1))
                 (assoc :index 6
                        :fixed [(op/invoke :a :read 0)
                                (op/invoke :b :write 1)
                                (op/invoke :a :read 1)]))})))

  (testing "Totally concurrent but corresponding reads/writes."
    (is (= (->> (linearizations (->Register 0)
                                [(op/invoke :w1 :write 1)
                                 (op/invoke :r1 :read 1)
                                 (op/invoke :w2 :write 2)
                                 (op/invoke :r2 :read 2)
                                 (op/invoke :w3 :write 3)
                                 (op/invoke :r3 :read 3)
                                 (op/ok :w1 :write 1)
                                 (op/ok :r1 :read 1)
                                 (op/ok :w2 :write 2)
                                 (op/ok :r2 :read 2)
                                 (op/ok :w3 :write 3)
                                 (op/ok :r3 :read 3)])
                (map :fixed)
                (map (partial map :process))
                set)
           #{[:w1 :r1 :w2 :r2 :w3 :r3]
             [:w1 :r1 :w3 :r3 :w2 :r2]
             [:w2 :r2 :w1 :r1 :w3 :r3]
             [:w2 :r2 :w3 :r3 :w1 :r1]
             [:w3 :r3 :w1 :r1 :w2 :r2]
             [:w3 :r3 :w2 :r2 :w1 :r1]}))))

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
                   (when (< crash-factor (rand)) (assert false))

                   (let [value (rand-int 5)]
                     (if (< (rand) 0.5)
                       ; Write
                       (do
                         (swap! history conj (op/invoke process :write value))
                         (set-mutable x value)
                         (when (< crash-factor (rand)) (assert false))
                         (swap! history conj (op/ok process :write value)))
                       ; Read
                       (do
                         (swap! history conj (op/invoke process :read nil))
                         (let [value (get-mutable x)]
                           (when (< crash-factor (rand)) (assert false))
                           (swap! history conj (op/ok process :read value)))))))
                 (catch AssertionError _ :crashed)))
    @history))

; Do a bunch of reads and writes on a volatile mutable variable and test if the
; resulting history is linearizable.
(deftest volatile-test
  (dotimes [i 1]
    (let [history (history/complete (volatile-history 6 100 0))
          linear  (linearizations (->Register 0) history)]
      (when (empty? linear)
        (clojure.pprint/pprint history)
        (clojure.pprint/pprint (linearizations (->Register 0) history)))
      (is (not (empty? linear))))))

(deftest prioqueue-test
  (let [q  (prioqueue/prioqueue awfulness-comparator)
        w1 (assoc (world :good) :index 10 :pending #{1})
        w2 (assoc (world :evil) :index 20 :pending #{1 2 3 4 5 6})]
    (->> [w1 w2 w1 w2]
         (map (partial prioqueue/put! q))
         (dorun))
    (is (= (take-while identity (repeatedly #(prioqueue/poll! q 1))))
        [w1 w1 w2 w2])))

(deftest history-a
  ; A CAS register history that exposes a bug
  (let [h [{:process 4, :type :invoke, :f :write, :value 2}
           {:process 4, :type :ok, :f :write, :value 2}
           {:process 6, :type :invoke, :f :read, :value nil}
           {:process 1, :type :invoke, :f :write, :value 4}
           {:process 1, :type :ok, :f :write, :value 4}
           {:process 14, :type :invoke, :f :read, :value 4}
           {:process 14, :type :ok, :f :read, :value 4}
           {:process 9, :type :invoke, :f :write, :value 0}
           {:process 9, :type :ok, :f :write, :value 0}
           {:process 19, :type :invoke, :f :read, :value 0}
           {:process 19, :type :ok, :f :read, :value 0}]
        a (analysis (->Register 0) h)]
    (is (:valid? a))
    (when-not (:valid? a)
      (pprint h)
      (println "history length" (count h))
      (prn)
      (pprint (update-in a [:worlds] (partial take 10))))))


(deftest volatile-linearizable-test
  (dotimes [i 1]
    (let [history (volatile-history 110 100 0.5)
          a       (analysis (->Register 0) history)]
      (is (:valid? a))
      (pprint history)
      (println "history length" (count history))
      (prn)
      (pprint (update-in a [:worlds] (partial take 10))))))
