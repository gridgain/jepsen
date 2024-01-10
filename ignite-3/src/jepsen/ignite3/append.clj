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
           (org.apache.ignite.table.mapper Mapper)
           (org.apache.ignite.tx TransactionException)))

(def table-name "APPEND")

(def max-attempts 20)

(def sql-create (str "create table if not exists " table-name "(key int primary key, vals varchar(1000))"))

(def sql-insert (str "insert into " table-name " (key, vals) values (?, ?)"))

(def sql-update (str "update " table-name " set vals = ? where key = ?"))

(def sql-select-all (str "select * from " table-name))

(def sql-select (str sql-select-all " where key = ?"))

(defprotocol Accessor
  "Provide transactional access to Ignite3 DB for read and append."
  (read!   [this ignite txn [opcode k v]] "Read value to the table by key, as list of numbers.")
  (append! [this ignite txn [opcode k v]] "Append value to the table by key."))

(defn run-sql
  "Run a SQL query. Return ResultSet instance that should be closed afterwards."
  ([session query params] (run-sql session nil query params))
  ([session txn query params]
    (log/info query params)
    (.execute session txn query (object-array params))))

(defn as-int-list [s]
  "Convert a string representation of integers into an actual list of integers.
   Empty string or nil are converted into an empty list.
   Non-integer content (including spaces) leads to parsing exception."
  (let [s' (if (some? s) s "")]
    (->> (clojure.string/split s' #",")
         (remove #(.isEmpty %))
         (mapv #(Integer/parseInt %)))))

(deftype SqlAccessor []
  Accessor
  ;
  (read! [this ignite txn [opcode k v]]
    (let [r (with-open [session   (.createSession (.sql ignite))
                        rs        (run-sql session txn sql-select [k])]
              (let [s (if (.hasNext rs) (.stringValue (.next rs) 1) "")]
                (as-int-list s)))]
      [:r k r]))
  ;
  (append! [this ignite txn [opcode k v]]
    (with-open [session   (.createSession (.sql ignite))
                read-rs   (run-sql session txn sql-select [k])]
      (if (.hasNext read-rs)
        ; update existing list
        (let [old-list    (.stringValue (.next read-rs) 1)
              new-list    (str old-list "," v)]
          (with-open [write-rs (run-sql session txn sql-update [new-list k])]))
        ; create a new list
        (with-open [write-rs (run-sql session txn sql-insert [k (str v)])]))
      [opcode k v])))

(defn kv-view [^Ignite ignite]
  "Create KV view for APPEND table."
  (.keyValueView (.table (.tables ignite) table-name)
                 (Mapper/of Integer)
                 (Mapper/of String)))

(deftype KeyValueAccessor []
  Accessor
  ;
  (read! [this ignite txn [opcode k v]]
    (let [view      (kv-view ignite)
          value     (as-int-list (.get view txn (int k)))]
      [:r k value]))
  ;
  (append! [this ignite txn [opcode k v]]
    (let [view      (kv-view ignite)
          old-value (.get view txn (int k))
          new-value (if (some? old-value)
                      (str old-value "," v)
                      (str v))]
      (.put view txn (int k) new-value)
      [opcode k v])))

(defn invoke-ops [^Ignite ignite acc ops]
  "Perform operations in a transaction."
  (let [txn (.begin (.transactions ignite))
        result (mapv #(case (first %)
                        :r       (read! acc ignite txn %)
                        :append  (append! acc ignite txn %))
                     ops)]
    (.commit txn)
    result))

(defn invoke-with-retries [^Ignite ignite acc ops]
  "Perform operations with repeats on IgniteException, each time in a new transaction."
  (loop [attempt 1]
    (if-let [r (try
                 (invoke-ops ignite acc ops)
                 (catch TransactionException te
                   (log/info "TransactionException:" (.getMessage te)))
                 (catch IgniteException ie
                   (if (.contains (.getMessage ie) "Failed to acquire a lock")
                     nil
                     (throw ie))))]
      r
      (do (log/info "Failed attempt" (str attempt "/" max-attempts) "for" ops)
          (if-not (< attempt max-attempts)
            (throw (RuntimeException. (str "Await exhausted after " max-attempts " attempts")))
            (do
              (Thread/sleep 50)
              (recur (inc attempt))))))))

(defn print-table-content [ignite]
  "Save resulting table content in the log."
  (with-open [session (.createSession (.sql ignite))
              rs (run-sql session sql-select-all [])]
    (log/info "Table content")
    (while (.hasNext rs)
      (let [row (.next rs)]
        (log/info (.intValue row 0) ":" (.stringValue row 1))))))

(defrecord Client [^Ignite ignite acc]
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
          ; result (invoke-ops ignite acc ops)
          result (invoke-with-retries ignite acc ops)
          overall-result (assoc op
                                :type :info
                                :value (into [] result))]
      overall-result))
  ;
  (teardown! [this test] (print-table-content ignite))
  ;
  (close! [this test]
    (.close ignite)))

(comment "for repl"

(def c (client/open! (Client. nil (KeyValueAccessor.)) {} "127.0.0.1"))
(client/setup! c {})

(client/invoke! c {} {:type :invoke, :process 0, :f :txn, :value [[:r 5 nil] [:r 6 nil]]})

(client/invoke! c {} {:type :invoke, :process 1, :f :txn, :value [[:append 9 2]]})

(client/invoke! c {} {:type :invoke, :process 0, :f :txn, :value [[:r 9 nil]]})

(client/teardown! c {})

(client/close! c {})

"/for repl"
)

(defn append-test
  [opts]
  (ignite3/basic-test
    (merge
      (let [test-ops {:consistency-models [:strict-serializable]}]
        {:name      "append-test"
         :client    (Client. nil (SqlAccessor.))
         :checker   (app/checker test-ops)
         :generator (ignite3/wrap-generator (app/gen test-ops) (:time-limit opts))})
      opts)))
