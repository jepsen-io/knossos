(ns knossos.wgl-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [knossos.wgl :refer :all]
            [knossos.linear :as linear]
            [knossos.op :as op]
            [knossos.model :as model :refer [register cas-register inconsistent]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]])
  (:import (knossos.model Model)))

(deftest ^:perf timeout-test
  (testing "will timeout if limit exceeded"
    (let [model (cas-register 0)
          history (ct/read-history-2 "data/cas-register/good/memstress3-12.edn")
          ;; Time limit of 1ms
          a (analysis model history {:time-limit 1})]
      (is (= {:valid?   :unknown
              :cause    :timeout
              :analyzer :wgl}
             a))))

  (testing "does not timeout when limit not exceeded"
    (let [model (register 0)
          history [(op/invoke 0 :write 1)
                   (op/ok     0 :write 1)]
          a (analysis model history {:time-limit 999999999999})]
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a))))

  (testing "does not timeout with no time-limit"
    (let [model (register 0)
          history [(op/invoke 0 :write 1)
                   (op/ok     0 :write 1)]
          a1 (analysis model history)
          a2 (analysis model history {:time-limit nil})]
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a1))
      (is (not= {:valid?   :unknown
                 :cause    :timeout}
                a2)))))

(deftest crash-test
  (let [model (register 0)
        history [(op/invoke 0 :write 1)
                 (op/info   0 :write 1)]
        a (analysis model history)]
    (is (:valid? a))))

(deftest sequential-test
  (let [a (analysis (register 0)
                    [(op/invoke 0 :read 0)
                     (op/ok     0 :read 0)])]
    (is (:valid? a))))

(deftest preserves-indices-test
  (let [model (register 0)
        history [{:process 0 :type :invoke :f :write :value 1 :index 5}
                 {:process 0 :type :ok     :f :write :value 1 :index 7}
                 {:process 0 :type :invoke :f :read :value 2 :index 99}
                 {:process 0 :type :ok     :f :read :value 2 :index 100}]
        a (analysis model history)]
    (is (= false (:valid? a)))
    (is (= :wgl (:analyzer a)))
    (is (= 100 (get-in a [:op :index])))
    (is (= 7   (get-in a [:previous-ok :index])))
    (let [final-paths (-> a :final-paths vec first)]
      (is (= 7   (-> final-paths first  :op :index)))
      (is (= 100 (-> final-paths second :op :index))))))

(deftest read-wrong-initial-value-test
  (let [model (register 0)
        history [{:process 0 :type :invoke :f :read :value 1}
                 {:process 0 :type :ok     :f :read :value 1}]
        a       (analysis model history)]
    (is (= {:analyzer    :wgl
            :valid?      false
            :op          {:process 0 :type :ok :f :read :value 1 :index 1}
            :previous-ok nil
            :final-paths #{[{:model model
                             :op nil}
                            {:model (inconsistent "0≠1")
                             :op {:f :read, :process 0 :type :ok :value 1 :index 1}}]}}
           a))))

(deftest bad-analysis-test
  (let [a (analysis
           (cas-register 0)
           (ct/read-history-2 "data/cas-register/bad/bad-analysis.edn"))]
    (is (= false (:valid? a)))
    (is (= {:process 21, :type :ok, :f :read, :value 2 :index 14} (:op a)))
    (is (= {:process 19, :type :ok, :f :read, :value 0 :index 11}
           (:previous-ok a)))))

(deftest bad-analysis-test-2
  (let [model (cas-register 0)
        a     (analysis model (ct/read-history-2 "data/cas-register/bad/cas-failure.edn"))]
    ; In this particular history, we know the value is 0, then we have
    ; concurrent reads of 0 and a write of 2 by process 76, followed by another
    ; read of 0 by process 70. The only legal linearization to that final read
    ; is all reads of 0, followed by 76 write 2, which leaves the state as 2.
    ; Process 70 read 0 should be the invalidating op.
    (is (= false (:valid? a)))

    ; We fail because we can't linearize the final read of 0
    (is (= {:process 70, :type :ok, :f :read, :value 0 :index 491}
           (:op a)))

     ; The last linearized ok was the completion of process 74's read of 0, but
     ; that's not the last linearized *op*: that'd be the write of 2.
    (is (= {:process 74, :type :ok, :f :read, :value 0 :index 478}
           (:previous-ok a)))

    (is (= #{[{:model (cas-register 2)
               :op {:f :write :process 76 :type :ok :value 2 :index 472}}
              {:model (inconsistent "can't CAS 2 from 1 to 1")
               :op {:f :cas :process 77 :type :info :value [1 1] :index 482}}]
             [{:model (cas-register 2)
               :op {:f :write :process 76 :type :ok :value 2 :index 472}}
              {:model (inconsistent "can't read 0 from register 2")
               :op {:f :read :process 70 :type :ok :value 0 :index 491}}]}
           (:final-paths a)))))

(deftest rethink-fail-minimal-test
  (let [a (analysis (cas-register nil)
                    (ct/read-history-2 "data/cas-register/bad/rethink-fail-minimal.edn"))]
    (is (= false (:valid? a)))

    ; We shouldn't be able to linearize the read of 3 by process 12, which
    ; appears out of nowhere.
    (is (= {:process 1, :type :ok, :f :read, :value 3 :index 4}
           (:op a)))))

(deftest rethink-fail-smaller-test
  (let [a (analysis (cas-register nil)
                    (ct/read-history-2 "data/cas-register/bad/rethink-fail-smaller.edn"))]
    (is (= false (:valid? a)))

    ; We shouldn't be able to linearize the read of 3 by process 12, which
    ; appears out of nowhere.
    (is (= {:process 12, :type :ok, :f :read, :value 3, :index 217 :time 106334571856}
           (:op a)))))

(deftest cas-failure-test
  (let [a (analysis (cas-register 0)
                    (ct/read-history-2 "data/cas-register/bad/cas-failure.edn"))]
    (is (= false (:valid? a)))
    (is (= {:f :read :process 70 :type :ok :value 0 :index 491}
           (:op a)))))

(deftest volatile-linearizable-test
  (dotimes [i 10]
    (let [history (ct/volatile-history 100 50 1/1000)
          a       (analysis (register 0) history)]
      (is (:valid? a)))))

(deftest ^:perf examples-test
  (ct/test-examples analysis))

;; ---------------------------------------------------------------------

(defrecord Log [entries])

(defn topic? [x] (string? x))
(defn data?  [x] (string? x))

(s/def ::entries  (s/coll-of data? :kind vector?))
(s/def ::log (s/keys :req-un [::entries]))

(defn initial-log
  []
  {:post [(s/valid? ::log %)]}
  (Log. (vector "network config"
                "membership/add n2"
                "membership/add n3"
                "membership/add n4"
                "membership/add n5")))

(defn append
  [log _topic _data]
  {:pre  [(s/valid? ::log log) (s/valid? data? _data)]
   :post [(s/valid? int? %)]}
  (inc (count (:entries log))))

(defn append-next
  [log _topic data]
  {:pre  [(s/valid? ::log log) (s/valid? string? data)]
   :post [(s/valid? ::log %)]}
  (Log. (conj (:entries log) data)))

(defn read-index
  [log index]
  {:pre  [(s/valid? ::log log) (s/valid? int? index)]
   :post [(s/valid? (s/or :data string? :not-found #{:not-found}) %)]}
  (get (:entries log) (dec index) :not-found))

(defn -read-index-next
  [log _index]
  {:pre  [(s/valid? ::log log) (s/valid? int? _index)]
   :post [(s/valid? ::log %)]}
  log)

(defn last-index
  [log]
  {:pre  [(s/valid? ::log log)]
   :post [(s/valid? int? %)]}
  (count (:entries log)))

(defn -last-index-next
  [log]
  {:pre  [(s/valid? ::log log)]
   :post [(s/valid? ::log %)]}
  log)

(defn parameters
  [op]
  (case (:f op)
    :append      [(-> op :value :topics) (-> op :value :data)]
    :read        [(-> op :value :index)]
    :status-last-index []))

(defn ap
  "Apply parameters."
  [f x op]
  (apply (partial f x) (parameters op)))

(defn mock
  "Given a pure model of the system and an operation, return a pure/mock response of
  what we expect that the real system to respond with if we would execute said
  operation against it."
  [op log]
  (case (:f op)
    :append            (ap append      log op)
    :read              (ap read-index  log op)
    :status-last-index (ap last-index  log op)))

(defn abstract-read
  [value]
  (if (= (:topics value) "membership")
    (let [command (case (-> value :data :data :command)
                    "add node"    "add"
                    "remove node" "remove"
                    (assert false
                            (str "abstract-read: impossible, invalid value: " value)))]

      (str "membership/" command " " (-> value :data :data :peer :name)))
    (-> value :data)))

(defn abstract
  "Abstracts away unnessesary details from the real system's response."
  [op]
  (case (:f op)
    :append      (or (-> op :value :indexes first)
                     :unavailable)
    :read        (-> op :value abstract-read)
    :status-last-index (-> op :value)))

(defn transition
  [log op]
  (case (:f op)
    :append            (ap append-next       log op)
    :read              (ap -read-index-next  log op)
    :status-last-index (ap -last-index-next  log op)))

(defn- mismatch
  [op mock real]
  (model/inconsistent
   (with-out-str
     (pprint {:op       (op/Op->map op)
              :expected mock
              :got      real}))))

(defrecord LogModel [log]
  Model
  (step [_model op]

    (let [mock (mock op log)
          real (abstract op)]

      (if (= mock real)
        (->LogModel (transition log op))
        (mismatch op mock real)))))

(deftest wgl-iterator-error
  (let [a (fn [] (analysis (LogModel. (initial-log))
                           (ct/read-history-2 "data/wgl-error.edn")))]
    (is (thrown-with-msg?
         java.lang.AbstractMethodError
         #"Method knossos/wgl/Op.iterator\(\)Ljava/util/Iterator; is abstract"
         (a)))))

(deftest wgl-iterator-error-shorter-history-passes
  (let [history (ct/read-history-2 "data/wgl-error.edn")
        history' (take 812 history)
        a (fn [] (analysis (LogModel. (initial-log)) history'))]
    (is (:valid? (a)))))

(deftest wgl-iterator-error-shorter-history-fails
  (let [history (ct/read-history-2 "data/wgl-error.edn")
        history' (take 813 history)
        a (fn [] (analysis (LogModel. (initial-log)) history'))]
    (is (thrown-with-msg?
         java.lang.AbstractMethodError
         #"Method knossos/wgl/Op.iterator\(\)Ljava/util/Iterator; is abstract"
         (a)))))

(deftest linear-works
  (let [a (linear/analysis (LogModel. (initial-log))
                           (ct/read-history-2 "data/wgl-error.edn"))]
    (is (:valid? a))))
