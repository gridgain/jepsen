(ns jepsen.ignite3
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]]))

(def db-dir "/home/zloddey/temp/QA-4202/ignite3-db-3.0.0-SNAPSHOT")
(def cli-dir "/home/zloddey/temp/QA-4202/ignite3-cli-3.0.0-SNAPSHOT")

(defn start!
  "Starts server for the given node."
  [node test]
  (info node "Starting server node")
  (c/cd db-dir (c/exec "bin/ignite3-db" "start"))
  (Thread/sleep 3000)
  (c/cd cli-dir (c/exec "bin/ignite3" "exec" "cluster" "init" "--cluster-name=ignite-cluster"
                        "--meta-storage-node=defaultNode")))