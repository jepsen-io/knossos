(defproject knossos "0.2.3"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :dependencies [[org.clojure/math.combinatorics "0.1.1"]
                 [org.clojure/core.typed.rt "0.3.16"]
                 [potemkin "0.3.4"]
                 [interval-metrics "1.0.0"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.6.1"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :test-selectors {:focus :focus}
  :jvm-opts ["-Xmx24g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
;             "-XX:-OmitStackTraceInFastThrow"
             "-server"]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [org.clojure/core.typed "0.3.16"]]}})
