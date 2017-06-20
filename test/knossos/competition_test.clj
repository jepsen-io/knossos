(ns knossos.competition-test
  (:require [clojure.test :refer :all]
            [knossos.competition :refer :all]
            [knossos.op :refer :all]
            [knossos.model :refer [cas-register register multi-register
                                   inconsistent]]
            [knossos.core-test :as ct]
            [clojure.pprint :refer [pprint]]))

(deftest example-test
  (ct/test-examples analysis))
