(defproject jepsen.afloatdb-server "0.1.0-SNAPSHOT"
  :description "A basic AfloatDB server"
  :url "http://microraft.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.1"]
                 [io.afloatdb/afloatdb-jepsen "1.0-SNAPSHOT"]]
  :main jepsen.afloatdb-server
  :aot [jepsen.afloatdb-server])
