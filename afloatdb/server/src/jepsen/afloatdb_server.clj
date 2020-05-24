(ns jepsen.afloatdb-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (io.afloatdb AfloatDB)
           (io.afloatdb.jepsen AfloatDBJepsen)))

          
(defn -main
  "run forest run"
  [& args]
  (let [ 
        ; Launch
        server      (AfloatDBJepsen/bootstrap)]
    (loop []
      (Thread/sleep 1000))))
