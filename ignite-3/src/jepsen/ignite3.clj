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

(defn list-nodes
  "Creates a list of nodes the current node should connect to."
  [all-nodes current-node]
  (let [other-nodes (remove #(= % current-node) all-nodes)]
    (if (seq other-nodes)
      (for [n other-nodes] (str "\"" n ":3344\""))
      ["\"localhost:3344\""])))

(defn node-name
  "Generates a default name for the given node."
  [all-nodes current-node]
  (str "node-" (inc (.indexOf all-nodes current-node))))

(defn configure-server!
  "Creates a server config file and uploads it to the given node."
  [test node]
  (let [all-nodes (:nodes test)]
    (c/exec :sed :-i (str "s/defaultNode/" (node-name all-nodes node) "/")
                     (str (db-dir test) "/etc/vars.env"))
    (c/exec :sed :-i (str "s/\"localhost:3344\"/" (clojure.string/join ", " (list-nodes all-nodes node)) "/")
                     (str (db-dir test) "/etc/ignite-config.conf"))))

(defn start!
  "Starts server for the given node."
  [test node]
  (info node "Starting server node")
  (c/cd (db-dir test) (c/exec "bin/ignite3db" "start"))
  (Thread/sleep 3000)
  (c/cd (cli-dir test)
        (c/exec "bin/ignite3"
                "cluster"
                "init"
                "--cluster-name=ignite-cluster"
                (str "--meta-storage-node=" (node-name (:nodes test) node))))
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
        (cu/install-archive! (:url test) server-dir)
        (configure-server! test node)
        (start! test node)))

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
       (gen/nemesis
         ; without "take 100", we fail into infinity here (most probably, during print)
         (take 100 (cycle [(gen/sleep 5)
                           {:type :info, :f :start}
                           (gen/sleep 1)
                           {:type :info, :f :stop}])))
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
