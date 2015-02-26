(ns knossos.util-test
  (:require [clojure.test :refer :all]
            [clojure.core.typed :refer [check-ns]]
            [knossos.util :refer :all]))

(deftest typecheck
  (is (check-ns 'knossos.util)))
