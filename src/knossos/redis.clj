(ns knossos.redis
  (:use knossos.core))

;; System state

(defn node
  "A node consists of a register, a primary it replicates from, whether it is
  isolated from all other nodes, a local replication offset, and if it is a
  primary, a map of node names to known replication offsets."
  [name]
  {:name name
   :register nil
   :primary  nil
   :isolated false
   :offset   0
   :offsets  {}})

(defn client
  "A client is a singlethreaded process which can, at any time, have at most
  one request in-flight to the cluster. It has a primary that it uses for reads
  and writes, and an in-flight request.
  
  Clients can be waiting for a response, in which case :wait will be the
  replication offset from the primary they're awaiting."
  [name]
  {:name    name
   :node    nil
   :waiting nil})

(defn coordinator
  "A controller is an FSM which manages the election process for nodes. It
  comprises a state (the phase of the election cycle it's in), and the current
  primary."
  [primary]
  {:state       :normal
   :primary     primary})

(defn system
  "A system is comprised of a collection of nodes, a collection of clients, and
  a coordinator; plus a *history*, which is the set of operations we're
  verifying is linearizable."
  []
  (let [node-names [:a :b :c]
        nodes      (->> node-names
                        (map node)
                        ; Fill in offset maps
                        (map (fn [node]
                               (->> node-names
                                    (remove #{(:name node)})
                                    (reduce #(assoc %1 %2 0) {})
                                    (assoc node :offsets)))))
        ; Initial primary/secondary state
        [primary & secondaries] nodes
        nodes (cons primary
                    (map #(assoc % :primary (:name primary))
                         secondaries))

        ; Construct a map of node names to nodes
        nodes (->> nodes
                   (map (juxt :name identity))
                   (into {}))

        ; Construct clients
        clients (->> [:a :b]
                     (map client)
                     (map #(assoc % :node (:name primary)))
                     (map (juxt :name identity))
                     (into {}))]
    {:coordinator (coordinator (:name primary))
     :clients     clients
     :nodes       nodes
     :history     []}))

;; Accessors

(defn get-client
  "Find a client by name."
  [system name]
  (-> system :clients (get name)))

(defn get-node
  "Find a node by name."
  [system name]
  (-> system :nodes (get name)))

(declare primary-node?)

(defn median [ns]
  ; Adapted from rosettacode
  (let [ns (sort ns)
        cnt (count ns)
        mid (bit-shift-right cnt 1)]
    (if (odd? cnt)
      (nth ns mid)
      (/ (+ (nth ns mid) (nth ns (dec mid))) 2))))

(defn majority-acked-offset
  "The highest offset known by a node to be acknowledged by a majority of
  nodes."
  [node]
  (assert (primary-node? node))
  (median (cons (:offset node)
                (vals (:offsets node)))))

;; Predicates

(defn primary-node?
  "Is a node a primary?"
  [node]
  (nil? (:primary node)))

(defn reachable-primary-node?
  "Is a node reachable (not isolated) and a primary?"
  [node]
  (and (primary-node? node)
       (not (:isolated node))))

(defn free-client?
  "Is a client able to initiate a request?"
  [client]
  (nil? (:waiting client)))

(defn valid-client?
  "Is the client connected to a primary node?"
  [system client]
  (and (:node client)
       (reachable-primary-node? (get-node system (:node client)))))

;; Useful subsets of the system

(defn primary-names
  "A set of node names which are primaries"
  [system]
  (->> system
      :nodes
      (keep (fn [[name node]]
              (when-not (:primary node)
                name)))
       set))

(defn clients
  "Given a system, returns all clients matching the given predicate."
  ([system]
   (vals (:clients system)))
  ([pred system]
   (->> system :clients vals (filter pred))))

(defn nodes
  "Given a system, returns all nodes matching the given predicate."
  ([system]
   (vals (:nodes system)))
  ([pred system]
   (->> system :nodes vals (filter pred))))


;; State transitions. Each transition takes a current state of the system and
;; returns a sequence of possible future states.

(def write-state (atom 0))

(defn client-write
  "A client can send a read or write operation to a node."
  [system]
  (->> system
       clients
       (filter free-client?)
       (filter (partial valid-client? system))
       (map (fn [client]
              (let [; Pick a value to write
                    value     (swap! write-state inc)
                    ; Find the node name for this client
                    node      (:node client)
                    ; And the new offset.
                    offset    (inc (get-in system [:nodes node :offset]))]
                (-> system
                    (assoc-in [:nodes node :register]            value)
                    (assoc-in [:nodes node :offset]              offset)
                    (assoc-in [:clients (:name client) :waiting] offset)
                    (assoc :history (conj (:history system)
                                          (invoke-op (:name client)
                                                     :write
                                                     value)))))))))

(defn client-write-complete
  "A reachable primary node can inform a client that its desired replication
  offset has been reached."
  [system]
  (->> system
       clients
       (remove free-client?)
       (filter (partial valid-client? system))
       (keep (fn [client]
               (let [offset (-> system
                                :nodes
                                (get (:node client))
                                majority-acked-offset)]
                 (when (<= (:waiting client) offset)
                   (-> system
                       (assoc-in [:clients (:name client) :waiting] nil)
                       (assoc :history (conj (:history system)
                                             (ok-op (:name client)
                                                    :write
                                                    nil))))))))))


(defn client-read
  "A client can read a value from its node, if primary and reachable. Reads are
  instantaneous."
  [system]
  (->> system
       clients
       (filter free-client?)
       (filter (partial valid-client? system))
       (map (fn [client]
              (let [node    (:node client)
                    value   (get-in system [:nodes node :register])
                    history (-> system
                                :history
                                (conj (invoke-op (:name client)
                                                 :read
                                                 nil))
                                (conj (ok-op (:name client)
                                             :read
                                             value)))]
                (assoc system :history history))))))

(defn replicate-from-primary
  "A node can copy the state of its current primary, if the primary is
  reachable."
  [system]
  (->> system
       nodes
       (remove :isolated)
       (keep (fn [node]
               (when-let [primary (get-node system (:primary node))]
                 (when-not (:isolated primary)
                   (-> system
                       (assoc-in [:nodes (:name node) :register]
                                 (:register primary))
                       (assoc-in [:nodes (:name node) :offset]
                                 (:offset primary)))))))))

(defn ack-offset-to-primary
  "A node can inform its current primary of its offset, if the primary is
  reachable."
  [system]
  (->> system
       nodes
       (remove :isolated)
       (keep (fn [node]
               (when-let [primary (get-node system (:primary node))]
                 (when-not (:isolated primary)
                   (assoc-in system [:nodes
                                     (:primary node)
                                     :offsets
                                     (:name node)]
                             (:offset node))))))))

(defn failover-1-isolate
  "If the coordinator is in normal mode, initiates failover by isolating the
  current primary."
  [system]
  (let [coord (:coordinator system)]
    (when (= :normal (:state coord))
      (-> system
          (assoc-in [:coordinator :state]               :isolated)
          (assoc-in [:coordinator :primary]             nil)
          (assoc-in [:nodes (:primary coord) :isolated] true)))))

(defn failover-2-select
  "If the coordinator has isolated the old primary, selects a new primary by
  choosing the reachable node with the highest offset."
  [system]
  (let [coord (:coordinator system)]
    (when (= :isolated (:state coord))
      (let [candidates (->> system nodes (remove :isolated))]
        ; Gotta reach a majority
        (assert (<= (inc (Math/floor (/ (count (nodes system)) 2)))
                    (count candidates)))
        (let [primary (:name (apply max-key :offset candidates))]
          (-> system
              (assoc-in [:coordinator :state] :selected)
              (assoc-in [:coordinator :primary] primary)))))))

(defn failover-3-inform-nodes
  "If the coordinator has selected a new primary, broadcasts that primary to
  all reachable nodes."
  [system]
  (let [coord   (:coordinator system)
        primary (:primary coord)]
    (when (= :selected (:state coord))
      (-> system
          (assoc-in [:coordinator :state] :informed-nodes)
          (assoc :nodes (->> system
                             :nodes
                             (map (fn [[name node]]
                                    [name
                                     (cond
                                       ; If the node is isolated, state is
                                       ; unchanged.
                                       (:isolated node)
                                       node

                                       ; If this is the new primary node, make
                                       ; it a primary.
                                       (= primary name)
                                       (assoc node :primary nil)

                                       ; Otherwise, set the primary.
                                       :else
                                       (assoc node :primary primary))]))
                             (into {})))))))

(defn failover-4-inform-clients
  "If the coordinator has informed all nodes of the new primary, update all
  client primaries."
  [system]
  (let [coord   (:coordinator system)
        primary (:primary coord)]
    (when (= :informed-nodes (:state coord))
      (-> system
          (assoc-in [:coordinator :state] :normal)
          (assoc :clients (->> system
                               :clients
                               (map (fn [[name client]]
                                      [name
                                       (assoc client :node primary)]))))))))

(defn failover
  "All four failover stages combined."
  [system]
  (if-let [system' (or (failover-1-isolate         system)
                       (failover-2-select          system)
                       (failover-3-inform-nodes    system)
                       (failover-4-inform-clients  system))]
    (list system')
    nil))

(defn step
  "All systems reachable in a single step from a given system."
  [system]
  (concat (client-write           system)
          (client-read            system)
          (replicate-from-primary system)
          (ack-offset-to-primary  system)
          (failover               system)
          (client-write-complete  system)))
