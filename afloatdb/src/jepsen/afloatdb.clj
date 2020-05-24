(ns jepsen.afloatdb
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian])
  (:import (io.afloatdb.client AfloatDBClient)
           (io.afloatdb.client.jepsen AfloatDBClientJepsen)
           (io.grpc StatusRuntimeException)
          ))


(def local-server-dir
  "Relative path to local server project directory"
  "server")


(def local-server-jar
  "Relative to server fat jar"
  (str local-server-dir
       "/target/jepsen.afloatdb-server-0.1.0-SNAPSHOT-standalone.jar"))


(def dir
  "Remote path for AfloatDB files"
  "/opt/afloatdb")


(def jar
  "Full path to AfloatDB server jar on remote nodes."
  (str dir "/server.jar"))


(def pid-file (str dir "/server.pid"))
(def log-file (str dir "/server.log"))


(defn build-server!
  "Ensures the server jar is ready."
  [test node]
  (when (= node (jepsen/primary test))
    (when-not (.exists (io/file local-server-jar))
      (info "Building server")
      (let [{:keys [exit out err]} (sh "lein" "uberjar" :dir "server")]
        (info out)
        (info err)
        (info exit)
        (assert (zero? exit))))))


(defn install!
  "Installs the server on remote nodes."
  []
  (c/cd dir
    (c/exec :mkdir :-p dir)
    (c/upload (.getCanonicalPath (io/file local-server-jar))
              jar)))


(defn start!
  "Launch AfloatDB server"
  [test node]
  (c/cd dir
        (cu/start-daemon!
          {:chdir dir
           :logfile log-file
           :pidfile pid-file}
          "/usr/bin/java"
          :-jar jar)))


(defn stop!
  "Kill AfloatDB server"
  [test node]
  (c/cd dir
        (c/su
          (cu/stop-daemon! pid-file))))


(defn db
  "Installs and runs AfloatDB nodes"
  []
  (reify db/DB
    (setup! [_ test node]
      (build-server! test node)
      (jepsen/synchronize test)
      (debian/install [:openjdk-8-jdk])
      (install!)
      (start! test node)
      (Thread/sleep 15000))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf log-file pid-file)))

    db/LogFiles
    (log-files [_ test node]
      [log-file])))


(defn ^AfloatDBClient connect
  "Creates a AfloatDB client for the given node."
  [node]
  (AfloatDBClientJepsen/newClient node))


(defn cas-register
  "Generates a CAS register with a key on the KV store"
  [client kv]
  (reify client/Client
    (setup! [_ test node]
      (let [client (connect node)]
        (cas-register client (.getKV client))))

    (invoke! [this test op]      
      (try
        (case (:f op)
        :read  (assoc op :type :ok, :value (.get kv "jepsen"))
        :write (do  (.set kv "jepsen" (:value op))
                    (assoc op :type :ok))
        :cas   (let [[currentV newV] (:value op)]
                (if (.replace kv "jepsen" currentV newV)
                  (assoc op :type :ok)
                  (assoc op :type :fail :error :cas-failed))))
        (catch StatusRuntimeException e 
          (let [status (.getStatus e)
                code (.getCode status)
                error-code (.value code)]
            (cond 
              ; resource exhausted
              (= 8  error-code) (assoc op :type :fail :error :cannot-replicate)
              ; failed precondition
              (= 9  error-code) (assoc op :type :fail :error :not-leader)
              ; deadline exceeded (indeterminate state). 
              ; no need to mark indeterminate reads as info
              (= 4   error-code) (if (= :read (:f op)) (assoc op :type :fail :error :indeterminate-read) (assoc op :type :info)) 
              ; unavailable. 
              ; no need to mark unavailable reads as info
              (= 14  error-code) (if (= :read (:f op)) (assoc op :type :fail :error :unavailable-read  ) (assoc op :type :info))
              :else (throw e)
            )))))

    (teardown! [this test]
      (.shutdown client))

      ))


(defn workload
  "Given a test map, computes

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}"
  [test]
  (case (:workload test)
    :cas-register             {:client    (cas-register nil nil)
                               :generator (->> (gen/mix [{:type :invoke, :f :read}
                                                        {:type :invoke, :f :write, :value (rand-int 5)}
                                                        ; (gen/sleep 1)
                                                        {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}])
                                           gen/each
                                          (gen/stagger 0.5))
                               :checker   (checker/linearizable {:model     (model/cas-register 0)})}
                       ))


(defn afloatdb-test
  "Constructs a Jepsen test map from CLI options"
  [opts]
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (workload opts)
        generator (->> generator
                       (gen/nemesis (gen/start-stop 15 10))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not final-generator
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis
                                  (gen/once {:type :info, :f :stop}))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 500)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name       (str "afloatdb " (name (:workload opts)))
            :os         debian/os
            :db         (db)
            :client     client
            ; :nemesis    (nemesis/partition-majorities-ring)
            :nemesis    (nemesis/partition-random-halves)
            ; :nemesis    nemesis/noop
            :generator  generator
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :stats    (checker/stats)
                           :timeline (timeline/html)
                           :workload checker})
            :model      model})))


(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. atomic-long-ids."
    :parse-fn keyword],
    [nil "--nemesis NEMESIS" "Nemesis type, e.g. partition-majorities-ring, partition-random-halves, noop"]])


(defn -main
  "Command line runner."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   afloatdb-test
                                         :opt-spec  opt-spec})
                   (cli/serve-cmd))
            args))
