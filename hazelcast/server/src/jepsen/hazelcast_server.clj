(ns jepsen.hazelcast-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (com.hazelcast.core Hazelcast)
           (com.hazelcast.config.raft RaftConfig
                                      RaftAlgorithmConfig
                                      RaftMetadataGroupConfig)
           (com.hazelcast.config Config
                                 LockConfig
                                 MapConfig
                                 QuorumConfig)))

(def opt-spec
  [["-m" "--members MEMBER-LIST" "Comma-separated list of peers to connect to"
    :parse-fn (fn [m]
                (str/split m #"\s*,\s*"))]])

(defn prepareRaftConfig
  "Prepare Hazelcast RaftConfig"
  [members]
  (let [raftAlgorithmConfig (RaftAlgorithmConfig.)
        metadataConfig (RaftMetadataGroupConfig.)
        raftConfig (RaftConfig.)

        _ (.setLeaderElectionTimeoutInMillis raftAlgorithmConfig 1000)
        _ (.setLeaderHeartbeatPeriodInMillis raftAlgorithmConfig 1500)
        _ (.setCommitIndexAdvanceCountToSnapshot raftAlgorithmConfig 250)
        ; _ (.setFailOnIndeterminateOperationState raftAlgorithmConfig true)

        _ (.setInitialRaftMember metadataConfig true)
        _ (.setGroupSize metadataConfig (count members))
        _ (.setMetadataGroupSize metadataConfig (count members))

        _ (.setRaftAlgorithmConfig raftConfig raftAlgorithmConfig)
        _ (.setMetadataGroupConfig raftConfig metadataConfig)
        _ (.setSessionHeartbeatIntervalMillis raftConfig 500)
        _ (.setSessionTimeToLiveSeconds raftConfig 5)
      ]
    raftConfig))

(defn -main
  "Go go go"
  [& args]
  (let [{:keys [options
                arguments
                summary
                errors]} (cli/parse-opts args opt-spec)
        config  (Config.)
        members (:members options)

        ; Timeouts
        _ (.setProperty config "hazelcast.client.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.heartbeat.interval.seconds" "1")
        _ (.setProperty config "hazelcast.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.operation.call.timeout.millis" "5000")
        _ (.setProperty config "hazelcast.wait.seconds.before.join" "0")
        _ (.setProperty config "hazelcast.merge.first.run.delay.seconds" "1")
        _ (.setProperty config "hazelcast.merge.next.run.delay.seconds" "1")

        ; Network config
        _       (.. config getNetworkConfig getJoin getMulticastConfig
                    (setEnabled false))
        tcp-ip  (.. config getNetworkConfig getJoin getTcpIpConfig)
        _       (doseq [member members]
                  (.addMember tcp-ip member))
        _       (.setEnabled tcp-ip true)

        ; prepare raft service
        _ (.setRaftConfig config (prepareRaftConfig members))

        ; Quorum for split-brain protection
        quorum (doto (QuorumConfig.)
                 (.setName "majority")
                 (.setEnabled true)
                 (.setSize (inc (int (Math/floor
                                       (/ (inc (count (:members options)))
                                          2))))))
        _ (.addQuorumConfig config quorum)


        ; Locks
        lock-config (doto (LockConfig.)
                      (.setName "jepsen.lock")
                      (.setQuorumName "majority"))
        _ (.addLockConfig config lock-config)

        ; Queues
        queue-config (doto (.getQueueConfig config "jepsen.queue")
                       (.setName "jepsen.queue")
                       (.setBackupCount 2)
                       (.setQuorumName "majority"))
        _ (.addQueueConfig config queue-config)


        ; Maps with CRDTs
        crdt-map-config (doto (MapConfig.)
                    (.setName "jepsen.crdt-map")
                    (.setMergePolicy
                      "jepsen.hazelcast_server.SetUnionMergePolicy"))
        _ (.addMapConfig config crdt-map-config)

        ; Maps without CRDTs
        map-config (doto (MapConfig.)
                     (.setName "jepsen.map")
                     (.setQuorumName "majority"))
        _ (.addMapConfig config map-config)

        ; Launch
        hc      (Hazelcast/newHazelcastInstance config)]
    (loop []
      (Thread/sleep 1000))))
