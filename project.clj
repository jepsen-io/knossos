(defproject knossos "0.3.1"
  :description "Linearizability checker"
  :url "https://github.com/aphyr/knossos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :main knossos.cli
  :dependencies [[org.clojure/math.combinatorics "0.1.1"]
                 [org.clojure/clojure "1.8.0"]
                 [potemkin "0.4.3"]
                 [interval-metrics "1.0.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.clojars.pallix/analemma "1.0.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.21"]]
  ; "-verbose:gc" "-XX:+PrintGCDetails"
  :test-selectors {:focus :focus}
  :jvm-opts ["-Xmx12g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-XX:+UnlockCommercialFeatures"
;             "-XX:-OmitStackTraceInFastThrow"
             "-server"
;             "-XX:+UnlockCommercialFeatures"
;             "-XX:+FlightRecorder"
;             "-XX:StartFlightRecording=delay=10s,duration=50s,filename=knossos.jfr"
]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}})
