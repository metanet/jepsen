(ns jepsen.hazelcast-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (com.hazelcast.core Hazelcast)
           (com.hazelcast.config Config
                                 LockConfig
                                 ServiceConfig
                                 MapConfig
                                 QuorumConfig)
           (com.hazelcast.raft RaftConfig
                               RaftMember)))

(def opt-spec
  [["-m" "--members MEMBER-LIST" "Comma-separated list of peers to connect to"
    :parse-fn (fn [m]
                (str/split m #"\s*,\s*"))]])

(defn prepareRaftServiceConfig
  "Prepare Hazelcast RaftConfig and ServiceConfig"
  [members]
  (let [raftConfig (RaftConfig.)
        serviceConfig (ServiceConfig.)

        ; add raft members
        memberlist (java.util.ArrayList.)
        _       (doseq [member members]
                  (info "Adding " member " to raft group")
                  (.add memberlist (RaftMember. (str member ":5701") member)))
        _ (.setMembers raftConfig memberlist)

        _ (.setLeaderElectionTimeoutInMillis raftConfig 1000)
        _ (.setLeaderHeartbeatPeriodInMillis raftConfig 1500)
        _ (.setCommitIndexAdvanceCountToSnapshot raftConfig 50)
        _ (.setAppendNopEntryOnLeaderElection raftConfig true)
        _ (.setMetadataGroupSize raftConfig (count members))

        ; prepare service config
        _ (.setEnabled serviceConfig true)
        _ (.setName serviceConfig com.hazelcast.raft.impl.service.RaftService/SERVICE_NAME)
        _ (.setClassName serviceConfig (.getName com.hazelcast.raft.impl.service.RaftService))
        _ (.setConfigObject serviceConfig raftConfig)
      ]
    serviceConfig))

(defn prepareAtomicLongServiceConfig
  "Prepare Raft AtomicLong service config"
  []
  (let [serviceConfig (ServiceConfig.)
        _ (.setEnabled serviceConfig true)
        _ (.setName serviceConfig com.hazelcast.raft.service.atomiclong.RaftAtomicLongService/SERVICE_NAME)
        _ (.setClassName serviceConfig (.getName com.hazelcast.raft.service.atomiclong.RaftAtomicLongService))
        ]
    serviceConfig))

(defn prepareLockServiceConfig
  "Prepare Raft Lock service config"
  []
  (let [serviceConfig (ServiceConfig.)
        _ (.setEnabled serviceConfig true)
        _ (.setName serviceConfig com.hazelcast.raft.service.lock.RaftLockService/SERVICE_NAME)
        _ (.setClassName serviceConfig (.getName com.hazelcast.raft.service.lock.RaftLockService))
        ]
    serviceConfig))

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

        ; prepare raft services
        servicesConfig (.getServicesConfig config)
        _ (.addServiceConfig servicesConfig (prepareRaftServiceConfig members))
        _ (.addServiceConfig servicesConfig (prepareAtomicLongServiceConfig))
        _ (.addServiceConfig servicesConfig (prepareLockServiceConfig))

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
