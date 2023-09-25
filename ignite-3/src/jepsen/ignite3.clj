(ns jepsen.ignite3
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]
             [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.centos :as centos]
            [jepsen.os.debian :as debian]))

(def server-dir "/opt/ignite3")

(defn db-dir
  [test]
  (str server-dir "/ignite3-db-" (:version test)))

(defn cli-dir
  [test]
  (str server-dir "/ignite3-cli-" (:version test)))

(defn ignite-url
  "Constructs the URL; either passing through the test's URL, or
  constructing one from the version."
  [test]
  (or (:url test)
      (str "http://192.168.1.74:8000/ignite3-" (:version test) ".zip")))

(defn start!
  "Starts server for the given node."
  [node test]
  (info node "Starting server node")
  (c/cd (db-dir test) (c/exec "bin/ignite3db" "start"))
  (Thread/sleep 3000)
  (c/cd (cli-dir test)
        (c/exec "bin/ignite3" "cluster" "init" "--cluster-name=ignite-cluster" "--meta-storage-node=defaultNode"))
  (Thread/sleep 3000))

(defn stop!
  "Shuts down server."
  [node test]
  (c/su
    (util/meh (c/exec :pkill :-9 :-f "org.apache.ignite.internal.app.IgniteRunner"))))

(defn nuke!
  "Shuts down server and destroys all data."
  [node test]
  (c/su
    (util/meh (c/exec :pkill :-9 :-f "org.apache.ignite.internal.app.IgniteRunner"))
    (c/exec :rm :-rf server-dir)))

(defn db
  "Apache Ignite 3 cluster life cycle."
  [version]
  (reify
    db/DB
    (setup! [_ test node]
      (info node "Installing Apache Ignite" version)
      (c/su
        (cu/install-archive! (ignite-url test) server-dir)
        (start! node test)))

    (teardown! [_ test node]
      (info node "Teardown Apache Ignite" version)
      (nuke! node test))

    db/LogFiles
    (log-files [_ test node]
      (list (str (db-dir test) "/log/ignite3db-0.log")))))

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
