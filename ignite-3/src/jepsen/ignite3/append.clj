(ns jepsen.ignite3.append
  "Append test.
  Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.tools.logging :as log]
            [jepsen [client :as client]
                    [ignite3 :as ignite3]
                    [nemesis :as nemesis]]
            [jepsen.tests.cycle.append :as app])
  (:import (org.apache.ignite Ignite)
           (org.apache.ignite.client IgniteClient)))

(def table-name "APPEND")

(def sql-create (str "create table if not exists " table-name "(key int primary key, vals varchar(1000))"))

(def sql-insert (str "insert into " table-name " (key, vals) values (?, ?)"))

(def sql-update (str "update " table-name " set vals = ? where key = ?"))

(def sql-select (str "select * from " table-name " where key = ?"))

(defn run-sql
  "Run a SQL query. Return ResultSet instance that should be closed afterwards."
  ([session query params] (run-sql session nil query params))
  ([session txn query params]
    (log/info query params)
    (.execute session txn query (object-array params))))

(defn invoke-op [^Ignite ignite [opcode k v]]
  "Perform a single operation in separate transaction."
  (let [tx  (.transactions ignite)
        sql (.sql ignite)]
    (if
      (= :r opcode)
      (do
        (let [select-result
               (with-open [session  (.createSession sql)
                           rs       (run-sql session sql-select [k])]
                 (if (.hasNext rs)
                   (let [raw-result (.stringValue (.next rs) 1)
                         strings    (clojure.string/split raw-result #",")]
                     (->> strings
                          (map #(Integer/parseInt %))
                          (into [])))
                   []))]
          [:r k select-result]))
      (let [txn (.begin tx)]
        (with-open [session   (.createSession sql)
                    read-rs   (run-sql session txn sql-select [k])]
          (if (.hasNext read-rs)
            ; update existing list
            (let [old-list    (.stringValue (.next read-rs) 1)
                  new-list    (str old-list "," v)]
              (with-open [write-rs (run-sql session txn sql-update [new-list k])]))
            ; create a new list
            (do
              (with-open [write-rs (run-sql session txn sql-insert [k (str v)])])))
          (.commit txn))
        [opcode k v]))))

(defrecord Client [^Ignite ignite]
  client/Client
  ;
  (open! [this test node]
    ; (log/info "Node: " node)
    (let [ignite (.build (.addresses (IgniteClient/builder) (into-array [(str node ":10800")])))]
      (assoc this :ignite ignite)))
  ;
  (setup! [this test]
    (with-open [create-stmt (.createStatement (.sql ignite) sql-create)
                session (.createSession (.sql ignite))
                rs (run-sql session create-stmt [])]
      (log/info "Table" table-name "created")))
  ;
  (invoke! [this test op]
    ; (log/info "Received: " op)
    (let [ops   (:value op)
          result (map #(invoke-op ignite %) ops)
          overall-result {:type :info, :f :txn, :value (into [] result)}]
      ; (log/info "Returned: " overall-result)
      overall-result))
  ;
  (teardown! [this test])
  ;
  (close! [this test]
    (.close ignite)))

(def pseudo-noop
  "Does nothing but logging."
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      ; (log/info "Nemesis received" op)
      (assoc op :type :info))
    (teardown! [this test] this)
    nemesis/Reflection
    (fs [this] #{})))

(comment "for repl"

(def c (client/open! (Client. nil) {} "127.0.0.1"))
(client/setup! c {})

(client/invoke! c {} {:type :invoke, :f :txn, :value [[:r 9 nil]]})

(client/invoke! c {} {:type :invoke, :f :txn, :value [[:append 9 4]]})

(client/close! c {})

"/for repl"
)

(defn append-test
  [opts]
  (ignite3/basic-test
    (merge
      (app/test {:consistency-models [:serializable]})
      {:name      "append-test"
       :client    (Client. nil)}
      opts)))
