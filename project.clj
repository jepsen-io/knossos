(defproject knossos "0.2"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {
                 "boundary-site" "http://maven.boundary.com/artifactory/repo"
                 }
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/math.combinatorics "0.0.7"]
                 [potemkin "0.3.4"]
                 [interval-metrics "1.0.0"]
                 [com.boundary/high-scale-lib "1.0.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.6.1"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC" "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts" "-XX:+UseFastAccessorMethods"])
