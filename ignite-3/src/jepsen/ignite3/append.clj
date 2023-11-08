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
           (org.apache.ignite.client IgniteClient)
           (org.apache.ignite.lang IgniteException)
           (org.apache.ignite.tx TransactionException)))

(def table-name "APPEND")

(def max-attempts 20)

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

(defn read! [ignite txn [_ k _]]
  "Read value to the table by key, as list of numbers."
  (let [r (with-open [session   (.createSession (.sql ignite))
                      rs        (run-sql session txn sql-select [k])]
            (let [s (if (.hasNext rs) (.stringValue (.next rs) 1) "")]
              (->> (clojure.string/split s #",")
                   (remove #(.isEmpty %))
                   (map #(Integer/parseInt %))
                   (into []))))]
    [:r k r]))

(defn append! [ignite txn [opcode k v]]
  "Append value to the table by key."
  (with-open [session   (.createSession (.sql ignite))
              read-rs   (run-sql session txn sql-select [k])]
    (if (.hasNext read-rs)
      ; update existing list
      (let [old-list    (.stringValue (.next read-rs) 1)
            new-list    (str old-list "," v)]
        (with-open [write-rs (run-sql session txn sql-update [new-list k])]))
      ; create a new list
      (with-open [write-rs (run-sql session txn sql-insert [k (str v)])]))
    [opcode k v]))

(def select-op {:r read! :append append!})

(defn invoke-op [^Ignite ignite op]
  "Perform a single operation in separate transaction."
  (let [txn (.begin (.transactions ignite))
        result ((get select-op (first op)) ignite txn op)]
    (.commit txn)
    result))

(defn invoke-with-retries [^Ignite ignite op]
  "Perform a single operation with repeats on IgniteException, each time in a new transaction."
  (loop [attempt 1]
    (if-let [r (try
                 (invoke-op ignite op)
                 (catch TransactionException te
                   (log/warn "TransactionException:" (.getMessage te)))
                 (catch IgniteException ie
                   (if (.contains (.getMessage ie) "Failed to acquire a lock")
                     nil
                     (throw ie))))]
      r
      (do (log/info "Failed attempt" (str attempt "/" max-attempts) "for" op)
          (if-not (< attempt max-attempts)
            (throw (RuntimeException. (str "Await exhausted after " max-attempts " attempts")))
            (do
              (Thread/sleep 50)
              (recur (inc attempt))))))))

(defrecord Client [^Ignite ignite]
  client/Client
  ;
  (open! [this test node]
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
    (let [ops   (:value op)
          ; result (map #(invoke-op ignite %) ops)
          result (map #(invoke-with-retries ignite %) ops)
          overall-result (assoc op
                                :type :info
                                :value (into [] result))]
      overall-result))
  ;
  (teardown! [this test])
  ;
  (close! [this test]
    (.close ignite)))

(comment "for repl"

(def c (client/open! (Client. nil) {} "127.0.0.1"))
(client/setup! c {})

(client/invoke! c {} {:type :invoke, :process 0, :f :txn, :value [[:r 9 nil]]})

(client/invoke! c {} {:type :invoke, :process 1, :f :txn, :value [[:append 9 2]]})

(client/close! c {})

"/for repl"
)

(defn append-test
  [opts]
  (ignite3/basic-test
    (merge
      (let [test-ops {:consistency-models [:serializable]}]
        {:name      "append-test"
         :client    (Client. nil)
         :checker   (app/checker test-ops)
         :generator (ignite3/wrap-generator (app/gen test-ops) (:time-limit opts))})
      opts)))
