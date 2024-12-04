(defproject knossos "0.3.11"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :main knossos.cli
  :dependencies [[org.clojure/math.combinatorics "0.2.0"]
                 [org.clojure/clojure "1.11.1"]
                 [potemkin "0.4.6"]
                 [slingshot "0.12.2"]
                 [interval-metrics "1.0.1"]
                 [org.clojure/tools.cli "1.0.219"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojars.pallix/analemma "1.0.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "1.2.4"]
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
