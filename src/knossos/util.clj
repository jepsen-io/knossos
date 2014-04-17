(ns knossos.util
  "Toolbox")

(defn rempty?
  "Like empty, but for reducibles."
  [coll]
  (reduce (fn [_ _] (reduced false)) true coll))

(defmacro with-thread-name
  "Sets the thread name for duration of block."
  [thread-name & body]
  `(let [old-name# (.. Thread currentThread getName)]
     (try
       (.. Thread currentThread (setName (name ~thread-name)))
       ~@body
       (finally (.. Thread currentThread (setName old-name#))))))
