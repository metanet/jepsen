(defproject jepsen.hazelcast "0.1.0-SNAPSHOT"
  :description "Jepsen tests for AfloatDB"
  :url "http://jepsen.io/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.1.18"]
                 [io.afloatdb/afloatdb-client-jepsen "1.0-SNAPSHOT"]]
  :main jepsen.afloatdb)
