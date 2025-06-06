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

(def join-path (partial clojure.string/join "/"))
(def join-comma (partial clojure.string/join ","))

(def server-dir "/opt/ignite3")

(defn db-dir
  "A path to the DB main directory, or its subpath (items in 'more')."
  [test & more]
  (join-path (concat [server-dir (str (:flavour test) "-db-" (:version test))]
                     more)))

(defn cli-dir
  "A path to the CLI main directory, or its subpath (items in 'more')."
  [test & more]
  (join-path (concat [server-dir (str (:flavour test) "-cli-" (:version test))]
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

(def config-name {"ignite3"     "ignite-config.conf"
                  "gridgain9"   "gridgain-config.conf"})

(defn configure-server!
  "Creates a server config file and uploads it to the given node."
  [test node]
  (let [all-nodes (:nodes test)]
    (c/exec :sed :-i (str "s/defaultNode/" (node-name all-nodes node) "/")
                     (db-dir test "etc" "vars.env"))
    (c/exec :sed :-i (str "s/\"localhost:3344\"/" (join-comma (list-nodes all-nodes node)) "/")
                     (db-dir test "etc" (get config-name (:flavour test))))))

(defn upload-wrapper!
  "Upload node startup wrapper to the node. TODO: try cu/start-daemon! instead"
  [test node]
  (info node "Upload startup wrapper")
  (c/upload "bin/start-wrapper.sh" (db-dir test "start-wrapper.sh")))

(defn env-from [test]
  "Gets environment settings from test, if any, or empty list otherwise."
  (let [e (:environment test)]
    (if (some? e) [:env e] [])))

(defn db-starter-name [test]
  "Extracts the name of DB executable for test, as a list."
  (list (get {"ignite3" "bin/ignite3db", "gridgain9" "bin/gridgain9db"}
             (:flavour test))))

(defn start-node!
  "Start a single Ignite node."
  [test node]
  (info node "Starting server node")
  (let [start-command (concat (env-from test)
                              ["sh" "start-wrapper.sh" (db-starter-name test)])]
    (c/cd (db-dir test) (apply c/exec start-command))))

(defn cli-starter-name [test]
  "Extracts the name of CLI utility for test, as a list."
  (list (get {"ignite3" "bin/ignite3", "gridgain9" "bin/gridgain9"}
             (:flavour test))))

(defn init-command [test]
  "Create a full 'ignite cluster init' CLI command."
  (concat (env-from test)
          (cli-starter-name test)
          ["cluster" "init" "--name=ignite-cluster"]
          (remove empty? (clojure.string/split (get test :extra-init-options "") #" "))))

(defn start!
  "Starts server for the given node."
  [test node]
  (upload-wrapper! test node)
  (start-node! test node)
  (Thread/sleep 10000)
  ; Cluster must be initialized only once
  (when (= 0 (.indexOf (:nodes test) node))
    (let [params (init-command test)]
      (info node "Init cluster as: " params)
      (c/cd (cli-dir test)
            (apply c/exec params)))
    (Thread/sleep 10000)))

(defn stop-node!
  [test node]
  (c/exec :pkill :-15 :-f "org.apache.ignite.internal.app.IgniteRunner"))

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
      (info node "Installing" (:flavour test) version)
      (c/su
        (c/exec :mkdir :-p server-dir)
        (c/exec :chown :-R (:username (:ssh test)) server-dir))
      (cu/install-archive! (:url test) (db-dir test))
      (when (= 0 (.indexOf (:nodes test) node))
        (cu/install-archive! (:cli-url test) (cli-dir test)))
      (configure-server! test node)
      (start! test node))

    (teardown! [_ test node]
      (info node "Teardown" (:flavour test) version)
      (nuke! test node))

    db/LogFiles
    (log-files [_ test node]
      (let [files (c/exec :find (db-dir test "log") :-type "f")]
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
