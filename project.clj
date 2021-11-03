(defproject knossos "0.3.9-SNAPSHOT"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :main knossos.cli
  :dependencies [[org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/clojure "1.10.3"]
                 [potemkin "0.4.5"]
                 [slingshot "0.12.2"]
                 [interval-metrics "1.0.0"]
                 [org.clojure/tools.cli "1.0.206"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojars.pallix/analemma "1.0.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "1.1.0"]
                 [metametadata/multiset "0.1.1"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :test-selectors {:default (complement :perf)
                   :perf :perf
                   :focus :focus}
  :jvm-opts ["-Xmx24g"
             "-server"
;             "-XX:-OmitStackTraceInFastThrow"
])
