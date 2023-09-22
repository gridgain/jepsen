(ns jepsen.ignite3
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
                    [db      :as db]
                    [generator :as gen]
                    [tests   :as tests]]
            [jepsen.os.centos :as centos]
            [jepsen.os.debian :as debian]))

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

(defn db
  "Apache Ignite 3 cluster life cycle."
  [version]
  (reify
    db/DB
    (setup! [_ test node]
      (info node "Installing Apache Ignite" version))

    (teardown! [_ test node]
      (info node "Teardown Apache Ignite" version))

    db/LogFiles
    (log-files [_ test node]
      [])))

(defn generator
  [operations time-limit]
  (->> (gen/mix operations)
       (gen/stagger 1/10)
       (gen/time-limit time-limit)))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [options]
  (info :opts options)
  (merge tests/noop-test
         (dissoc options :test-fns)
         {:name    "basic-test"
          :os      (case (:os options)
                     :centos centos/os
                     :debian debian/os
                     :noop jepsen.os/noop)
          :db      (db (:version options))
          :pds     (:pds options)
          :nemesis (:nemesis options)}))
