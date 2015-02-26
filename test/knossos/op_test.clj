(ns knossos.op-test
  (:require [clojure.test :refer :all]
            [clojure.core.typed :refer [check-ns]]
            [knossos.op :refer :all]))

(deftest typecheck
  (is (check-ns 'knossos.op)))
