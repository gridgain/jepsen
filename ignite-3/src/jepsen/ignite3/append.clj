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
                    [nemesis :as nemesis]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as app]
            [knossos.model :as model])
  (:import (org.apache.ignite Ignite)
           (org.apache.ignite.client IgniteClient)))

(def table-name "APPEND")

(def sql-create (str "create table if not exists " table-name "(key int primary key, vals varchar(1000))"))

(def sql-insert (str "insert into " table-name " (key, vals) values (?, ?)"))

(def sql-update (str "update " table-name " set vals = ? where key = ?"))

(def sql-select (str "select * from " table-name " where key = ?"))

(defn invoke-op [^Ignite ignite [opcode k v]]
  "Perform a single operation in separate transaction"
  (let [tx  (.transactions ignite)
        sql (.sql ignite)]
    (if
      (= :r opcode)
      (do
        (log/info sql-select k)
        (let [select-result
               (with-open [session  (.createSession sql)
                           rs       (.execute session nil sql-select (into-array [k]))]
                 (if (.hasNext rs)
                   (into [] (map #(Integer/parseInt %) (clojure.string/split (.stringValue (.next rs) 1) #",")))
                   []))]
          [:r k select-result]))
      (let [txn (.begin tx)]
        (with-open [session   (.createSession sql)
                    read-rs   (.execute session txn sql-select (into-array [k]))]
          (if (.hasNext read-rs)
            ; update existing list
            (let [old-list    (.stringValue (.next read-rs) 1)
                  new-list    (str old-list "," v)]
              (log/info sql-update new-list k)
              (with-open [write-rs (.execute session txn sql-update (object-array [new-list k]))]))
            ; create a new list
            (do
              (log/info sql-insert k v)
              (with-open [write-rs (.execute session txn sql-insert (object-array [k (str v)]))])))
          (.commit txn))
        [opcode k v]))))

(defrecord Client [^Ignite ignite]
  client/Client
  ;
  (open! [this test node]
    (log/info "Node: " node)
    (let [ignite (.build (.addresses (IgniteClient/builder) (into-array [(str node ":10800")])))]
      (assoc this :ignite ignite)))
  ;
  (setup! [this test]
    (with-open [create-stmt (.createStatement (.sql ignite) sql-create)
                session (.createSession (.sql ignite))
                rs (.execute session nil create-stmt (object-array []))]
      (log/info "Table" table-name "created")))
  ;
  (invoke! [this test op]
    (log/info "Received: " op)
    (let [ops   (:value op)
          tx    (.transactions ignite)
          sql   (.sql ignite)
          result (map #(invoke-op ignite %) ops)
          overall-result {:type :info, :f :txn, :value (into [] result)}]
      (log/info "Returned: " overall-result)
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
      (log/info "Nemesis received" op)
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
      {:name      "append-test"
       :client    (Client. nil)
       :checker   (app/checker opts)
       :generator (app/gen opts)}
      opts)))
