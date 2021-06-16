(ns knossos.cli
  "Command-line verification of edn histories."
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [knossos [competition :as competition]
                     [history :as history]
                     [linear :as linear]
                     [wgl :as wgl]
                     [model :as model]])
  (:gen-class)
  (:import (java.io PushbackReader)))

(defn read-history
  "Takes a filename and loads a history from it, in EDN format. If an initial [
  or ( is present, loads the entire file in one go as a collection, expecting
  it to be a collection of op maps. If the initial character is {, loads it
  piecewise as op maps."
  [file]
  (let [h (with-open [r (PushbackReader. (io/reader file))]
            (->> (repeatedly #(edn/read {:eof nil} r))
                 (take-while identity)
                 vec))
        ; Handle the presence or absence of an enclosing vector
        h (if (and (= 1 (count h))
                   (sequential? (first h)))
            (vec (first h))
            h)]
    ; Normalize ops
    (history/parse-ops h)))

(def models
  {"register"     model/register
   "cas-register" model/cas-register
   "mutex"        model/mutex})

(def algos
  {"competition" competition/analysis
   "wgl"         wgl/analysis
   "linear"      linear/analysis})

(def opts
  "tools.cli options"
  [["-a" "--algorithm ALGO"
    :default competition/analysis
    :parse-fn algos
    :validate [identity
               (str "Must be one of " (str/join ", " (sort (keys algos))))]]
   ["-m" "--model MODEL"
    :default  model/cas-register
    :parse-fn models
    :validate [identity
               (str "Must be one of " (str/join ", " (sort (keys models))))]]])

(defn -main
  [& args]
  (try
    (let [{:keys [options arguments summary errors]} (cli/parse-opts args opts)
          analysis (:algorithm options)]
      (when-not (empty? errors)
        (doseq [e errors]
          (println e))
        (System/exit 1))

      (let [model ((:model options))]
        (doseq [file arguments]
          (print file)
          (flush)
          (let [history  (read-history file)
                ; _ (pprint history)
                analysis (analysis model history)]
            (print "\t")
            (pprint analysis)
            (prn (:valid? analysis)))))
      (System/exit 0))

    (catch Throwable t
      (println)
      (println)
      (println "Oh jeez, I'm sorry. Something went wrong.")
      (.printStackTrace t)
      (System/exit 255))))
