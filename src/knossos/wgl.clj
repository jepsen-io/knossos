(ns knossos.wgl
  "An implementation of the Wing and Gong linearizability checker, plus Lowe's
  extensions, as described in

  http://www.cs.cmu.edu/~wing/publications/WingGong93.pdf
  http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf
  https://arxiv.org/pdf/1504.00204.pdf"
  (:require [knossos [analysis :as analysis]
                     [history :as history]
                     [op :as op]
                     [search :as search]
                     [util :refer [deref-throw]]
                     [model :as model]]
            [knossos.wgl.dll-history :as dllh]
            [knossos.model.memo :as memo :refer [memo]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.set :as set]
            [potemkin :refer [definterface+ deftype+]])
  (:import (knossos.wgl.dll_history Node INode)
           (knossos.model.memo Wrapper)
           (clojure.lang MapEntry)
           (java.util HashSet
                      BitSet
                      ArrayDeque
                      Set)))

; We spend a lot of time in arraymap scans; we'll replace those with our own
; dedicated Op type.
(deftype+ Op [process type f value ^:int index ^:int entry-id m]
  ; We can't use defrecord because it computes hashcodes via
  ; APersistentMap/mapHashEq which is really expensive, and we can't override
  ; hashcode without breaking defrecord.
  clojure.lang.IKeywordLookup
  (getLookupThunk [this k]
                  (let [gclass (class this)]
                    (condp identical? k
                      :process (reify clojure.lang.ILookupThunk
                                 (get [thunk gtarget]
                                   (if (identical? (class gtarget) gclass)
                                     (.-process ^Op gtarget)
                                     thunk)))
                      :type (reify clojure.lang.ILookupThunk
                              (get [thunk gtarget]
                                (if (identical? (class gtarget) gclass)
                                  (.-type ^Op gtarget)
                                  thunk)))
                      :f (reify clojure.lang.ILookupThunk
                           (get [thunk gtarget]
                             (if (identical? (class gtarget) gclass)
                               (.-f ^Op gtarget)
                               thunk)))
                      :value (reify clojure.lang.ILookupThunk
                               (get [thunk gtarget]
                                 (if (identical? (class gtarget) gclass)
                                   (.-value ^Op gtarget)
                                   thunk)))
                      :index (reify clojure.lang.ILookupThunk
                               (get [thunk gtarget]
                                 (if (identical? (class gtarget) gclass)
                                   (.-index ^Op gtarget)
                                   thunk)))
                      :entry-id (reify clojure.lang.ILookupThunk
                                  (get [thunk gtarget]
                                    (if (identical? (class gtarget) gclass)
                                      (.-entry-id ^Op gtarget)
                                      thunk))))))

  ; sigh
  clojure.lang.Associative
  (entryAt [this k]
           (condp identical? k
             :process  (MapEntry/create k process)
             :type     (MapEntry/create k type)
             :f        (MapEntry/create k f)
             :value    (MapEntry/create k value)
             :index    (MapEntry/create k index)
             :entry-id (MapEntry/create k entry-id)
                       (.entryAt ^clojure.lang.Associative m k)))

  ; le sigh
  clojure.lang.ILookup
  (valAt [this k]
         (condp identical? k
           :process  process
           :type     type
           :f        f
           :value    value
           :index    index
           :entry-id entry-id
                     (.valAt ^clojure.lang.ILookup m k)))

  clojure.lang.IPersistentMap
  (assoc [this k v]
         (condp identical? k
           :process   (Op. v       type f value index entry-id m)
           :type      (Op. process v    f value index entry-id m)
           :f         (Op. process type v value index entry-id m)
           :value     (Op. process type f v     index entry-id m)
           :index     (Op. process type f value v     entry-id m)
           :entry-id  (Op. process type f value index v        m)
                      (Op. process type f value index entry-id
                           (assoc m k v))))

  (without [this k]
    (condp identical? k
           :process   (Op. nil     type f   value index entry-id m)
           :type      (Op. process nil  f   value index entry-id m)
           :f         (Op. process type nil value index entry-id m)
           :value     (Op. process type f   nil   index entry-id m)
           :index     (Op. process type f   value nil   entry-id m)
           :entry-id  (Op. process type f   value index nil      m)
                      (Op. process type f   value index entry-id
                           (dissoc m k))))

  clojure.lang.IHashEq
  (hasheq [this]
          (-> (hash process)                        (unchecked-multiply-int 37)
              (unchecked-add-int (hash type))       (unchecked-multiply-int 37)
              (unchecked-add-int (hash f))          (unchecked-multiply-int 37)
              (unchecked-add-int (hash value))      (unchecked-multiply-int 37)
              (unchecked-add-int (or index -1))     (unchecked-multiply-int 37)
              (unchecked-add-int (or entry-id -1))  (unchecked-multiply-int 37)
              (unchecked-add-int (hash m))))

  clojure.lang.IPersistentCollection
  (equiv [this other]
         (boolean
           (or (identical? this other)
               (and (identical? (class this) (class other))
                    (= index    (.-index    ^Op other))
                    (= entry-id (.-entry-id ^Op other))
                    (= process  (.-process  ^Op other))
                    (= type     (.-type     ^Op other))
                    (= f        (.-f        ^Op other))
                    (= value    (.-value    ^Op other))
                    (= m        (.-m        ^Op other))))))

  clojure.lang.ISeq
  (seq [this]
       (->> (seq m)
            (cons (MapEntry/create :entry-id entry-id))
            (cons (MapEntry/create :index    index))
            (cons (MapEntry/create :value    value))
            (cons (MapEntry/create :f        f))
            (cons (MapEntry/create :type     type))
            (cons (MapEntry/create :process  process))))

  Object
  (hashCode [this]
          (-> (.hashCode process)                   (unchecked-multiply-int 37)
              (unchecked-add-int (.hashCode type))  (unchecked-multiply-int 37)
              (unchecked-add-int (.hashCode f))     (unchecked-multiply-int 37)
              (unchecked-add-int (.hashCode value)) (unchecked-multiply-int 37)
              (unchecked-add-int index)             (unchecked-multiply-int 37)
              (unchecked-add-int entry-id)          (unchecked-multiply-int 37)
              (unchecked-add-int (.hashCode m))))

  (equals [this other]
         (boolean
           (or (identical? this other)
               (and (identical? (class this) (class other))
                    (.equals index    (.-index    ^Op other))
                    (.equals entry-id (.-entry-id ^Op other))
                    (.equals process  (.-process  ^Op other))
                    (.equals type     (.-type     ^Op other))
                    (.equals f        (.-f        ^Op other))
                    (.equals value    (.-value    ^Op other))
                    (.equals m        (.-m        ^Op other))))))

  (toString [this]
            (str "Op{:process " process
                 ", :type " type
                 ", :f " f
                 ", :value " value
                 ", :index " index
                 ", :entry-id " entry-id
                 (->> m
                      (map (fn [pair]
                             (str (key pair) " " (val pair))))
                      (str/join ", ")))))

(prefer-method clojure.pprint/simple-dispatch
               clojure.lang.IPersistentMap
               clojure.lang.ISeq)

(defn map->Op
  "Construct an Op from a map."
  [m]
  (Op. (:process  m)
       (:type     m)
       (:f        m)
       (:value    m)
       (:index    m)
       (:entry-id m)
       (dissoc m :process :type :f :value :index :entry-id)))

(defn Op->map
  "Turns an Op back into a plain old map, stripping its index and entry ids."
  [^Op op]
  (when op
    (into
      {:process (.process op)
       :type    (.type op)
       :f       (.f op)
       :value   (.value op)}
      (.m op))))

; This is the datatype we use to prune our exploration of the search space. We
; assume the linearized BitSet is immutable, memoizing its hashing to optimize
; both hash and equality checks. Op is only present for failure reconstruction
; and does not participate in equality/hashing checks.
(deftype+ CacheConfig [^BitSet linearized
                       model
                       op
                       ^:volatile-mutable ^int hasheq]
  clojure.lang.IHashEq
  (hasheq [ps]
          (when (= -1 hasheq)
            (set! hasheq (int (bit-xor (hash linearized)
                                       (hash model)))))
          hasheq)

  Object
  (hashCode [this]
            (.hasheq this))

  (equals [this other]
          (or (identical? this other)
              (and (identical? (class this) (class other))
                   (= hasheq      (.hasheq      ^CacheConfig other))
                   (= model       (.model       ^CacheConfig other))
                   (= linearized  (.linearized  ^CacheConfig other))))))

(defn linearized [^CacheConfig c]
  (.linearized c))

(defn cache-config [linearized model op]
  (CacheConfig. linearized model op -1))


(defn with-entry-ids
  "Assigns a sequential :entry-id to every invocation and completion. We're
  going to pick these ids in a sort of subtle way, because when it comes time
  to identify the point where we got stuck in the search, we want to be able to
  look at a linearized bitset and quickly identify the highest OK operation we
  linearized. To that end, we assign ok invocations the range 0..c, and crashed
  operations c+1..n, such that contiguous ranges of linearized ops represent
  progress through every required OK operation in the history."
  [history]
  (loop [h'     (transient []) ; New history
         calls  (transient {}) ; A map of processes to the indices of invokes.
         i      0              ; Index into the history
         e-ok   0              ; Next entry ID
         ; entry-ids for infos
         e-info (- (count history)
                   (count (history/crashed-invokes history)))]
    (if (<= (count history) i)
      ; OK, done!
      (do (assert (= 0 (count calls)))
          (persistent! h'))

      ; Normal op processing
      (let [op (nth history i)
            p  (:process op)]
        (cond (op/invoke? op)
              (do (assert (not (get calls p)))
                  (recur (assoc! h' i op)
                         (assoc! calls p i)
                         (inc i)
                         e-ok
                         e-info))

              (op/ok? op)
              (let [invoke-i (get calls p)
                    invoke (nth h' invoke-i)]
                (recur (-> h'
                           (assoc! invoke-i (assoc invoke :entry-id e-ok))
                           (assoc! i        (assoc op     :entry-id e-ok)))
                       (dissoc! calls p)
                       (inc i)
                       (inc e-ok)
                       e-info))

              (op/fail? op)
              (assert false (str "Err, shouldn't have a failure here: "
                                 (pr-str op)))

              (op/info? op)
              (if-let [invoke-i (get calls p)]
                ; This info corresponds to an ealier invoke
                (let [invoke (nth h' invoke-i)]
                  (recur (-> h'
                             (assoc! invoke-i (assoc invoke :entry-id e-info))
                             (assoc! i        (assoc op     :entry-id e-info)))
                         (dissoc! calls p)
                         (inc i)
                         e-ok
                         (inc e-info)))

                ; Just a random info
                (recur (assoc! h' i op)
                       calls
                       (inc i)
                       e-ok
                       e-info)))))))

(defn max-entry-id
  "What's the highest entry ID in this history?"
  [history]
  (reduce max -1 (keep :entry-id history)))

(defn bitset-highest-contiguous-linearized
  "What's the highest entry-id this BitSet has linearized, such that every
  lower entry ID is also linearized? -1 if nothing linearized."
  [^BitSet b]
  (dec (.nextClearBit b 0)))

(defn configurations-which-linearized-up-to
  "Filters a collection of configurations to only those which linearized every
  operation up to and including the given entry id."
  [entry-id configs]
  (filter (fn [^CacheConfig config]
            (<= entry-id
                (bitset-highest-contiguous-linearized (.linearized config))))
          configs))

(defn highest-contiguous-linearized-entry-id
  "Finds the highest linearized entry id given a set of CacheConfigs. -1 if no
  entries linearized. Because our search is speculative, we may actually try
  linearizing random high entry IDs even though we can't linearize smaller
  ones. For that reason, we use the highest *contiguous* entry ID."
  [configs]
  (->> configs
       (map linearized)
       (map bitset-highest-contiguous-linearized)
       (reduce max -1)))

(defn invoke-and-ok-for-entry-id
  "Given a history and an entry ID, finds the [invocation completion] operation
  for that entry ID."
  [history entry-id]
  (let [found (reduce (fn [invoke op]
                        (if invoke
                          (if (= (:process invoke) (:process op))
                            (reduced [invoke op])
                            invoke)
                          (if (= entry-id (:entry-id op))
                            op
                            invoke)))
                      nil
                      history)]
    (assert (vector? found))
    found))

(defn history-without-linearized
  "Takes a history and a linearized bitset, and constructs a sequence over that
  history with already linearized operations removed."
  [history ^BitSet linearized]
  (loop [history  (seq history)
         history' (transient [])
         calls    (transient #{})]
    (if-not history
      (persistent! history')
      (let [op       (first history)
            process  (:process op)
            entry-id (:entry-id op)]
        (cond ; This entry has been linearized; skip it and its completion.
              (and entry-id (.get linearized entry-id))
              (recur (next history) history' (conj! calls process))

              ; This entry is a return from a call that was already linearized.
              (get calls process)
              (recur (next history) history' (disj! calls process))

              ; Something else
              true
              (recur (next history) (conj! history' op) calls))))))

(defn concurrent-ops-at
  "Given a history and a particular operation in that history, computes the set
  of invoke operations concurrent with that particular operation."
  [history target-op]
  (loop [history    (seq history)
         concurrent (transient {})]
    (if-not history
      (vals (persistent! concurrent))
      (let [op      (first history)
            process (:process op)]
        (cond ; Found our target
              (= op target-op)
              (vals (persistent! concurrent))

              ; Start a new op
              (op/invoke? op)
              (recur (next history) (assoc! concurrent process op))

              ; Crashes
              (op/info? op)
              (recur (next history) concurrent)

              ; ok/fail
              true
              (recur (next history) (dissoc! concurrent process)))))))


(defn final-paths
  "We have a raw history, a final ok op which was nonlinearizable, and a
  collection of cache configurations. We're going to compute the set of
  potentially concurrent calls at the nonlinearizable ok operation.

  Then we'll filter the configurations to those which got the furthest into the
  history. Each config tells us the state of the system after linearizing a set
  of operations. We take that state and apply all concurrent operations to it,
  in every possible order, ending with the final op which broke
  linearizability."
  [history pair-index final-op configs]
  (let [calls (concurrent-ops-at history final-op)]
    (->> configs
         (map (fn [^CacheConfig config]
                (let [linearized ^BitSet (.linearized config)
                      model (.model config)
                      calls (->> calls
                                 (remove (fn [call]
                                           (.get linearized (:entry-id call))))
                                 (mapv pair-index))]
                  (analysis/final-paths-for-config
                    [{:op    (pair-index (.op config))
                      :model model}]
                    final-op
                    calls))))
         (reduce set/union)
         ; Unwrap memoization/Op wrappers
         (map (fn [path]
                (mapv (fn [transition]
                        (let [m (:model transition)
                              m (if (instance? Wrapper m)
                                  (memo/model m)
                                  m)
                              o (Op->map (:op transition))]
                          {:op o :model m}))
                      path)))
         set)))

(defn invalid-analysis
  "Constructs an analysis of an invalid terminal state."
  [history configs]
  (let [pair-index     (history/pair-index+ history)
        ; We failed because we ran into an OK entry. That means its invocation
        ; couldn't be linearized. What was the entry ID for that invocation?
        final-entry-id (inc (highest-contiguous-linearized-entry-id configs))
        ;_ (prn :final-entry-id final-entry-id)

        ; From that we can find the invocation and ok ops themselves
        [final-invoke
         final-ok]  (invoke-and-ok-for-entry-id history final-entry-id)
        ;_ (prn :final-invoke final-invoke)
        ;_ (prn :final-ok     final-ok)

        ; Now let's look back to the previous ok we *could* linearize
        previous-ok (analysis/previous-ok history final-ok)

        configs (->> configs
                     (configurations-which-linearized-up-to
                       (dec final-entry-id)))
        ;_     (prn :configs)
        ;_     (pprint configs)

        ; Now compute the final paths from the previous ok operation to the
        ; nonlinearizable one
        final-paths (final-paths history pair-index final-ok configs)]
    {:valid?      false
     :op          (Op->map final-ok)
     :previous-ok (Op->map previous-ok)
     :final-paths final-paths}))

(defn check
  [model history state]
  (let [history     (->> history
                         history/complete
                         history/with-synthetic-infos
                         history/without-failures
                         history/index
                         with-entry-ids
                         (mapv map->Op))
        {:keys [model history]} (memo model history)
        ; _ (pprint history)
        n           (max (max-entry-id history) 0)
        head-entry  (dllh/dll-history history)
        linearized  (BitSet. n)
        cache       (doto (HashSet.)
                      (.add (cache-config (BitSet.) model nil)))
        calls       (ArrayDeque.)]
    (loop [s              model
           ^Node entry    (.next head-entry)]
        (cond
          ; Manual abort!
          (not (:running? @state))
          {:valid? :unknown
           :cause  (:cause @state)}

          ; We've linearized every operation in the history
          (not (.next head-entry))
          {:valid? true
           :model  s}

          true
          (let [op ^Op (.op entry)
                type (.type op)]
            (condp identical? type
              ; This is an invocation. Can we apply it now?
              :invoke
              (let [s' (model/step s op)]
                (if (model/inconsistent? s')
                  ; We can't apply this right now; try the next entry
                  (recur s (.next entry))

                  ; OK, we can apply this to the current state
                  (let [; Cache that we've explored this configuration
                        entry-id    (.entry-id op)
                        linearized' ^BitSet (.clone linearized)
                        _           (.set linearized' entry-id)
                        ;_           (prn :linearized op)
                        ; Very important that we don't mutate linearized' once
                        ; we've wrapped it in a CacheConfig!
                        new-config? (.add cache
                                          (cache-config linearized' s' op))]
                    (if (not new-config?)
                      ; We've already been here, try skipping this invocation
                      (let [entry (.next entry)]
                        (recur s entry))

                      ; We haven't seen this state yet
                      (let [; Record this call so we can backtrack
                            _           (.addFirst calls [entry s])
                            ; Clean up
                            s           s'
                            _           (.set linearized entry-id)
                            ; Remove this call and its return from the
                            ; history
                            _           (dllh/lift! entry)
                            ; Start over from the start of the shortened
                            ; history
                            entry       (.next head-entry)]
                        (recur s entry))))))

              ; This is an :ok operation. If we *had* linearized the invocation
              ; already, this OK would have been removed from the chain by
              ; `lift!`, so we know that we haven't linearized it yet, and this
              ; is a dead end.
              :ok
              (if (.isEmpty calls)
                ; We have nowhere left to backtrack to.
                (invalid-analysis history cache)

                ; Backtrack, reverting to an earlier state.
                (let [[^INode entry s]  (.removeFirst calls)
                      op                ^Op (.op entry)
                      ; _                 (prn :revert op :to s)
                      _                 (.set linearized ^long (.entry-id op)
                                              false)
                      _                 (dllh/unlift! entry)
                      entry             (.next entry)]
                  (recur s entry)))

              ; We reordered all the infos to the end in dll-history, which
              ; means that at this point, there won't be any more invoke or ok
              ; operations. That means there are no more chances to fail in the
              ; search, and we can conclude here. The sequence of :invoke nodes
              ; in our dll-history is exactly those operations we chose not to
              ; linearize, and each corresponds to an info here at the end of
              ; the history.
              :info
              {:valid? true
               :model  s}))))))

(defn start-analysis
  "Spawn a thread to check a history. Returns a Search."
  [model history]
  (let [state   (atom {:running? true})
        results (promise)
        worker  (Thread.
                  (fn []
                    (try
                      (deliver results
                               (check model history state))
                      (catch InterruptedException e
                        (let [{:keys [cause]} @state]
                          (deliver results
                                   {:valid? :unknown
                                    :cause  cause})))
                      (catch Throwable t
                        (deliver results t)))))]

    (.start worker)
    (reify search/Search
      (abort! [_ cause]
        (swap! state assoc
               :running? false
               :cause cause))

      (report [_]
        [:WGL])

      (results [_]
        (deref-throw results))
      (results [_ timeout timeout-val]
        (deref-throw results timeout timeout-val)))))

(defn analysis
  "Given an initial model state and a history, checks to see if the history is
  linearizable. Returns a map with a :valid? bool and additional debugging
  information."
  [model history]
  (search/run (start-analysis model history)))
