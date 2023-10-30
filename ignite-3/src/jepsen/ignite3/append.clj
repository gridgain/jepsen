(ns jepsen.ignite3.append
  "Append test.
  Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.tools.logging :as log]
            [jepsen [checker :as checker]
                    [client :as client]
                    [ignite3 :as ignite3]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as app]
            [knossos.model :as model])
  (:import (org.apache.ignite.client IgniteClient)))

(def table-name "REGISTER")

(def sql-create (str"create table if not exists " table-name "(key varchar primary key, val int)"))

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

  (invoke! [this test op]
    ; TODO: implement
    (log/info op))

  (teardown! [this test])

  (close! [this test]
    (.close ignite)))

(defn test
  [opts]
  (ignite3/basic-test
    (merge
      {:name      "append-test"
       :client    (Client. nil)
       :checker   (app/checker opts)
       :generator (app/gen opts)}
      opts)))
