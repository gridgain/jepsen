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
  "Creates path to the DB main directory, or its subpath (items in 'more')."
  [test & more]
  (clojure.string/join "/" (concat [server-dir (str (:flavour test) "-db-" (:version test))]
                                   more)))

(defn cli-dir
  "Creates path to the CLI main directory, or its subpath (items in 'more')."
  [test & more]
  (clojure.string/join "/" (concat [server-dir (str (:flavour test) "-cli-" (:version test))]
                                   more)))

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
                     (db-dir test "etc" "vars.env"))
    (c/exec :sed :-i (str "s/\"localhost:3344\"/" (clojure.string/join ", " (list-nodes all-nodes node)) "/")
                     (db-dir test "etc" "ignite-config.conf"))))

(defn start-node!
  "Start a single Ignite node"
  [test node]
  (info node "Starting server node")
  (c/cd (db-dir test) (c/exec "bin/ignite3db" "start")))

(defn start!
  "Starts server for the given node."
  [test node]
  (start-node! test node)
  (Thread/sleep 3000)
  ; Cluster must be initialized only once
  (when (= 0 (.indexOf (:nodes test) node))
    (let [nodes     (:nodes test)
          ; use 1 CMG for cluster of 1-3 nodes, and 3 CMG for larger clusters
          cmg-size  (if (< 3 (count nodes)) 3 1)
          cmg-nodes (take cmg-size nodes)
          params    (clojure.string/join " "
                      (concat
                        [(str "--meta-storage-node=" (node-name nodes node))]
                        (mapv #(str "--cmg-node " (node-name nodes %)) cmg-nodes)))]
      (info node "Init cluster with params: " params)
      (c/cd (cli-dir test)
            (c/exec "bin/ignite3"
                    "cluster"
                    "init"
                    "--cluster-name=ignite-cluster"
                    (str "--meta-storage-node=" (node-name nodes node)))))
    (Thread/sleep 3000)))

(defn stop-node!
  [test node]
  (c/cd (db-dir test) (c/exec "bin/ignite3db" "stop")))

(defn stop!
  "Shuts down server."
  [test node]
  (info node "Shutting down server node")
  (util/meh (stop-node! test node)))

(defn nuke!
  "Shuts down server and destroys all data."
  [test node]
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
      (nuke! test node))

    db/LogFiles
    (log-files [_ test node]
      (let [files (c/exec :find (db-dir test "log") :-type "f" :-name "ignite3*")]
        (info node files)
        (into [] (.split files "\n"))))))

(defn wrap-generator
  "Add default wrapper for generator (frequency, nemesis, time limit)."
  [generator time-limit]
  (->> generator
       (gen/stagger 1/10)
       (gen/nemesis
         (cycle [(gen/sleep 30)
                 {:type :info, :f :start}
                 (gen/sleep 30)
                 {:type :info, :f :stop}]))
       (gen/time-limit time-limit)))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [options]
  (merge tests/noop-test
         (dissoc options :test-fns)
         {:os      (case (:os options)
                     :centos centos/os
                     :debian debian/os
                     :noop jepsen.os/noop)
          :db      (db (:version options))
          :nemesis (:nemesis options)}))
