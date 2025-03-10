(defproject knossos "0.3.13-SNAPSHOT"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :main knossos.cli
  :dependencies [[org.clojure/math.combinatorics "0.3.0"]
                 [org.clojure/clojure "1.12.0"]
                 [potemkin "0.4.7"]
                 [slingshot "0.12.2"]
                 [interval-metrics "1.0.1"]
                 [org.clojure/tools.cli "1.1.230"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojars.pallix/analemma "1.0.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "1.3.0"]
                 [metametadata/multiset "0.1.1"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :test-selectors {:default (complement :perf)
                   :perf :perf
                   :focus :focus}
  :profiles {:dev {:dependencies [[criterium "0.4.6"]]}}
  :jvm-opts ["-Xmx24g"
             "-server"
;             "-XX:-OmitStackTraceInFastThrow"
])
