(ns knossos.search.proto
  "Polymorphic interface for managing search lifecycles.")

(defprotocol Search
  (abort!   [search cause])
  (report!  [search])
  (results  [search]))
