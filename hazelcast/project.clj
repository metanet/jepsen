(defproject jepsen.hazelcast-raft "0.1.0-SNAPSHOT"
  :description "Jepsen tests for hazelcast-raft"
  :url "http://jepsen.io/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["jitpack" "https://jitpack.io"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.9"]
                 [com.github.mdogan.hazelcast/hazelcast-raft-client "raft-extended-SNAPSHOT"]]
  :main jepsen.hazelcast)

;; To avoid strict TSL check ("Tried to use insecure HTTP repository without TLS").
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
 "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))
