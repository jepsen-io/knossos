(ns knossos.wgl.dll-history
  "A history traversal structure built around a mutable unsynchronized doubly
  linked list which supports O(1) removal and insertion of nodes."
  (:refer-clojure :exclude [next])
  (:require [clojure.core :as c]
            [knossos.history :as history]
            [knossos.op :as op]
            [clojure.tools.logging :refer [info warn]]
            [potemkin :refer [definterface+ deftype+]]))

(definterface+ INode
  (^knossos.wgl.dll_history.INode prev [n])
  (^knossos.wgl.dll_history.INode next [n])
  (op [n])
  (^knossos.wgl.dll_history.INode match [n])
  (^knossos.wgl.dll_history.INode setPrev [n prev])
  (^knossos.wgl.dll_history.INode setNext [n next])
  (^knossos.wgl.dll_history.INode setMatch [n match])
  (node-seq [n]))

(deftype+ Node [^:unsynchronized-mutable prev
                ^:unsynchronized-mutable next
                op
                ^:unsynchronized-mutable match]
  knossos.wgl.dll_history.INode
  (prev   [_] prev)
  (next   [_] next)
  (op     [_] op)
  (match  [_] match)

  (setPrev  [this p] (set! prev p) this)
  (setNext  [this n] (set! next n) this)
  (setMatch [this m] (set! match m) this)

  (node-seq [this]
            (cons this (when-let [n next]
                         (lazy-seq (node-seq n)))))

  clojure.lang.Seqable
  (seq [this]
       (if (nil? prev)
         ; We're a head node, skip us
         (seq next)
         ; OK regular node
         (cons op (when-let [n next]
                    (lazy-seq (seq n))))))

  java.lang.Object
  (toString [this]
    (str "#Node" {:op op, :match match})))


(defn ^INode dll-history
  "Constructs a double-linked-list history from any other type of history.
  Returns a head node whose next entry is the first entry in the history."
  [history]
  (let [head (Node. nil nil nil nil)]
    (loop [history    (seq history)
           calls      (transient {})
           infos      (transient [])
           ^Node prev head]
      (if history
        (let [op (first history)]
          (cond
            ; For invokes, we append a node to the list and record a call entry
            (op/invoke? op)
            (let [node (Node. prev nil op nil)]
              (.setNext prev node)
              (recur (c/next history)
                     (assoc! calls (:process op) node)
                     infos
                     node))

            ; On completion, append a node to the list and fill in our invoke
            ; node's match
            (op/ok? op)
            (let [node   (Node. prev nil op nil)
                  invoke (get calls (:process op))]
              (.setNext prev node)
              (.setMatch ^Node invoke node)
              (recur (c/next history)
                     (dissoc! calls (:process op))
                     infos
                     node))

            ; We don't do failure
            (op/fail? op)
            (throw (IllegalArgumentException.
                     "Can't compute dll-histories over histories with :fail ops"))

            ; Save infos for later
            (op/info? op)
            (recur (c/next history) calls (conj! infos op) prev)))

        ; OK we're out of history elements! Append info nodes
        (loop [calls      calls
               infos      (seq (persistent! infos))
               ^Node prev prev]
          (if infos
            ; Append an info node
            (let [op (first infos)]
              (if-let [invoke (get calls (:process op))]
                ; We have an invocation
                (let [node (Node. prev nil op nil)]
                  (.setNext prev node)
                  (.setMatch ^Node invoke node)
                  (recur (dissoc! calls (:process op))
                         (c/next infos)
                         node))
                ; No invocation, skip
                (recur calls (c/next infos) prev)))

            ; OK we're done, but just to make sure
            (let [calls (persistent! calls)]
              (when-not (empty? calls)
                (info "Expected all invocations to have a matching :ok or :info, but invocations by processes "
                      (pr-str (keys calls))
                      " went unmatched. This might indicate a malformed history, but we're going to go ahead and check it anyway by inserting :info events for these uncompleted invocations.")
                (recur (transient calls)
                       (map (fn [[process invoke-node]]
                              (let [op (.op invoke-node)]
                                (op/info process (:f op) (:value op))))
                            calls)
                       prev)))))))

    head))

(defn lift!
  "Excises a node from the history by stitching together its next and previous
  nodes, and likewise for its match."
  [^Node entry]
  (let [prev  ^INode (.prev entry)
        next  ^INode (.next entry)]
    (.setNext prev next)
    (.setPrev next prev)
    (when-let [match ^INode (.match entry)]
      (.setNext ^INode (.prev match) (.next match))
      (when-let [n ^INode (.next match)]
        (.setPrev n (.prev match))))))

(defn unlift!
  "Adds a node back into the history by linking its next and previous back to
  where they should be, and the same for its match."
  [^Node entry]
  (when-let [match ^INode (.match entry)]
    (.setNext ^INode (.prev match) match)
    (when-let [n ^INode (.next match)]
      (.setPrev n match)))
  (.setNext ^INode (.prev entry) entry)
  (.setPrev ^INode (.next entry) entry))
