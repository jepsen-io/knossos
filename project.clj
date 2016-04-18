(defproject knossos "0.2.5-SNAPSHOT"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :dependencies [[org.clojure/math.combinatorics "0.1.1"]
                 [potemkin "0.4.3"]
                 [interval-metrics "1.0.0"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojars.pallix/analemma "1.0.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.21"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :test-selectors {:focus :focus}
  :jvm-opts ["-Xmx24g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-XX:-OmitStackTraceInFastThrow"
             "-server"]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}})
