(ns knossos.core-test
  (:require [clojure.test :refer :all]
            [knossos.core :refer :all]
            [clojure.pprint :refer [pprint]]))

(deftest complete-test
  (testing "empty history"
    (is (= (complete [])
           [])))

  (testing "an invocation"
    (is (= (complete [(invoke-op :a :read nil)])
           [(invoke-op :a :read nil)])))

  (testing "a completed invocation"
    (is (= (complete [(invoke-op :a :read nil)
                      (ok-op     :a :read 2)])
           [(invoke-op :a :read 2)
            (ok-op     :a :read 2)])))

  (testing "a failed invocation"
    (is (= (complete [(invoke-op :a :read nil)
                      (fail-op   :a :read 2)])
           (complete [(invoke-op :a :read 2)
                      (fail-op   :a :read nil)])
           [(invoke-op :a :read 2)
            (fail-op   :a :read 2)])))

  (testing "an unbalanced set of invocations"
    (is (thrown? RuntimeException
                 (complete [(invoke-op :a :read nil)
                            (invoke-op :a :read nil)]))))

  (testing "an unbalanced completion"
    (is (thrown? AssertionError
                 (complete [(ok-op :a :read 2)])))))

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
  (let [ops [(ok-op :a :read 0)
             (ok-op :a :read nil)
             (ok-op :a :write 1)
             (ok-op :a :read 1)]]
    (is (= (-> (->Register 0)
               world
               (assoc :pending (set ops))
               (advance-world ops))
           (assoc (world (->Register 1)) :fixed ops)))

    (is (inconsistent-world?
                 (advance-world (world (->Register 0))
                                [(ok-op :a :read 1)])))))

(deftest degenerate-worlds-test
  (testing "empty"
    (is (= (degenerate-worlds #{}) #{})))

  (testing "singular"
    (is (= (degenerate-worlds
             #{{:model 2 :pending #{1 2 3}}})
           #{{:model 2 :pending #{1 2 3}}})))

  (testing "distinct models"
    (is (= (degenerate-worlds
             #{{:model 1 :pending #{1 2 3}}
               {:model 2 :pending #{1 2 3}}})
           #{{:model 1 :pending #{1 2 3}}
             {:model 2 :pending #{1 2 3}}})))

  (testing "same models"
    (is (= (->>
             #{{:model 1 :fixed :a :pending #{1 2 3}}
               {:model 1 :fixed :b :pending #{1 2 3}}
               {:model 1 :fixed :a :pending #{1 4}}
               {:model 1 :fixed :b :pending #{1 4}}
               {:model 1 :fixed :a :pending #{}}
               {:model 1 :fixed :b :pending #{}}}
             degenerate-worlds
             (map #(select-keys % [:model :pending]))
             set)
           #{{:model 1 :pending #{1 2 3}}
             {:model 1 :pending #{1 4}}
             {:model 1 :pending #{}}}))))

(deftest possible-worlds-test
  (testing "empty"
    ; The projection of an empty world is an empty world. I'm so alone.
    (is (= (possible-worlds (world (->Register 1)))
           [(world (->Register 1))])))

  (testing "fixed history"
    (let [w (-> (world (->Register 0))
                (assoc :fixed [(ok-op :a :read 0)]))]
      (is (= (possible-worlds w) [w]))))

  (testing "one fixed, one pending operation"
    (let [w (-> (world (->Register 0))
                (assoc :fixed    [(ok-op :a :read 0)])
                (assoc :pending #{(ok-op :a :write 1)}))]
      (is (= (set (possible-worlds w))
             ; In one world, the write occurs
             #{(-> (world (->Register 1))
                   (assoc :fixed [(ok-op :a :read 0)
                                  (ok-op :a :write 1)])
                   (assoc :pending #{}))
               ; And in the other, the write is still pending.
               w}))))

  (testing "Multiple pending operations with only two linear paths"
    (let [w (-> (world (->Register 0))
                (assoc :pending #{(ok-op :a :read 0)
                                  (ok-op :a :write 1)
                                  (ok-op :a :read 1)}))]
      (is (= (set (possible-worlds w))
             #{; Nothing happened
               w
               ; Read 0 happened
               (-> w
                   (assoc :fixed   [(ok-op :a :read 0)])
                   (assoc :pending #{(ok-op :a :write 1)
                                     (ok-op :a :read 1)}))
               ; Read 0 and write 1 happened.
               (-> w
                   (assoc :model   (->Register 1))
                   (assoc :fixed   [(ok-op :a :read 0)
                                    (ok-op :a :write 1)])
                   (assoc :pending #{(ok-op :a :read 1)}))
               ; All ops happened
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(ok-op :a :read 0)
                                  (ok-op :a :write 1)
                                  (ok-op :a :read 1)])
                   (assoc :pending #{}))
               ; Write 1 happened.
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(ok-op :a :write 1)])
                   (assoc :pending #{(ok-op :a :read 0)
                                     (ok-op :a :read 1)}))
               ; Write 1 and read 1 happened.
               (-> w
                   (assoc :model (->Register 1))
                   (assoc :fixed [(ok-op :a :write 1)
                                  (ok-op :a :read 1)])
                   (assoc :pending #{(ok-op :a :read 0)}))})))))

(deftest linearizations-test
  (testing "Empty history"
    (is (= (set (linearizations (->Register 0) []))
           #{(world (->Register 0))})))

  (testing "Single invocation and completion."
    (is (= (set (linearizations (->Register 0)
                                [(invoke-op :a :read 0)
                                 (ok-op :a :read 0)]))
           #{(-> (world (->Register 0))
                 (assoc :index 2
                        :fixed [(invoke-op :a :read 0)]))})))

  (testing "Single invocation and failure."
    (is (= (set (linearizations (->Register 0)
                                [(invoke-op :a :read 0)
                                 (fail-op   :a :read 0)]))
           #{(-> (world (->Register 0))
                 (assoc :index 2))})))

  (testing "Simple read-write race with one linearization."
    (is (= (set (linearizations (->Register 0)
                                [(invoke-op :a :read 0)
                                 (ok-op :a :read 0)
                                 (invoke-op :a :read 1)   ; Overlaps B's write
                                 (invoke-op :b :write 1)
                                 (ok-op :a :read 1)
                                 (ok-op :b :write 1)]))
           #{(-> (world (->Register 1))
                 (assoc :index 6
                        :fixed [(invoke-op :a :read 0)
                                (invoke-op :b :write 1)
                                (invoke-op :a :read 1)]))})))

  (testing "Totally concurrent but corresponding reads/writes."
    (is (= (->> (linearizations (->Register 0)
                                [(invoke-op :w1 :write 1)
                                 (invoke-op :r1 :read 1)
                                 (invoke-op :w2 :write 2)
                                 (invoke-op :r2 :read 2)
                                 (invoke-op :w3 :write 3)
                                 (invoke-op :r3 :read 3)
                                 (ok-op :w1 :write 1)
                                 (ok-op :r1 :read 1)
                                 (ok-op :w2 :write 2)
                                 (ok-op :r2 :read 2)
                                 (ok-op :w3 :write 3)
                                 (ok-op :r3 :read 3)])
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

; Do a bunch of reads and writes on a volatile mutable variable and test if the
; resulting history is linearizable.
(deftest volatile-test
  (dotimes [i 1]
    (let [history (atom [])
          x       (VolatileVariable. 0)]
      (dothreads [process 6]
        (dotimes [i 100]
          (Thread/sleep (rand-int 5))
          (let [value (rand-int 5)]
            (if (< (rand) 0.5)
              ; Write
              (do
                (swap! history conj (invoke-op process :write value))
                (set-mutable x value)
                (swap! history conj (ok-op process :write value)))
              ; Read
              (do
                (swap! history conj (invoke-op process :read nil))
                (let [value (get-mutable x)]
                  (swap! history conj (ok-op process :read value))))))))
      (let [history (complete @history)
            linear  (linearizations (->Register 0) history)]
        (prn (count history))
        (when (empty? linear)
          (clojure.pprint/pprint history)
          (clojure.pprint/pprint (linearizations (->Register 0) history)))
        (is (not (empty? linear)))))))
