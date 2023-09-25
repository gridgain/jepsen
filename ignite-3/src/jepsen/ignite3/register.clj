(ns jepsen.ignite3.register
  "Single atomic register test"
  (:require [clojure.tools.logging :as log]
            [jepsen [checker :as checker]
                    [client :as client]
                    [ignite3 :as ignite3]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model])
  (:import (org.apache.ignite.client IgniteClient)))

(def table-name "REGISTER")

(def sql-create (str"create table if not exists " table-name "(key varchar primary key, val int)"))

(defn r
  "read operation"
  [_ _] {:type :invoke, :f :read, :value nil})

(defn w
  "write operation"
  [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn cas
  "compare and set operation"
  [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [ignite]
  client/Client

  (open! [this test node]
    (log/info "Node: " node)
    (let [ignite (.build (.addresses (IgniteClient/builder) (into-array [(str node ":10800")])))
          create-stmt (.createStatement (.sql ignite) sql-create)]
      (with-open [session (.createSession (.sql ignite))
                  rs (.execute session nil create-stmt (into-array []))]
        (log/info "Table" table-name "created"))
      (assoc this :ignite ignite)))

  (setup! [this test])
  (invoke! [this test op])
  (teardown! [this test])
  (close! [this test]))

(defn test
  [opts]
  (ignite3/basic-test
    (merge
      {:name      "register-test"
       :client    (Client. nil)
       :checker   (independent/checker
                    (checker/compose
                      {:linearizable (checker/linearizable {:model (model/cas-register)})
                       :timeline  (timeline/html)}))
       :generator (ignite3/generator [r w cas] (:time-limit opts))}
      opts)))