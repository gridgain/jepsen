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
  (:import (org.apache.ignite               Ignite)
           (org.apache.ignite.client        IgniteClient
                                            IgniteClientConnectionException
                                            RetryLimitPolicy)
           (org.apache.ignite.sql           Statement SqlException)
           (org.apache.ignite.table.mapper  Mapper)
           (org.apache.ignite.tx            TransactionException)))

; ---------- Common definitions ----------

(def table-name "APPEND")

(defn sql-create-zone
  "Create replication zone with a given amount of table replicas"
  [replicas]
  (str "create zone if not exists \"" table-name "_zone\" with replicas=" replicas))

(def sql-create (str "create table if not exists " table-name "(key int primary key, vals varchar(1000))"
                     " with PRIMARY_ZONE='" table-name "_zone'"))

(def sql-select-all (str "select * from " table-name))

(defprotocol Accessor
  "Provide transactional access to Ignite3 DB for read and append."
  (read!   [this ignite txn [opcode k v]] "Read value to the table by key, as list of numbers.")
  (append! [this ignite txn [opcode k v]] "Append value to the table by key."))

(defn run-sql
  "Run a SQL query. Return ResultSet instance that should be closed afterwards."
  ([sql query params] (run-sql sql nil query params))
  ([sql txn query params]
    (let [printable-query (if (instance? Statement query)
                              (.query query)
                              query)]
      (log/info printable-query params))
    (.execute sql txn query (object-array params))))

(defn as-int-list [s]
  "Convert a string representation of integers into an actual list of integers.
   Empty string or nil are converted into an empty list.
   Non-integer content (including spaces) leads to parsing exception."
  (let [s' (if (some? s) s "")]
    (->> (clojure.string/split s' #",")
         (remove #(.isEmpty %))
         (mapv #(Integer/parseInt %)))))

; ---------- SQL Access ----------

(def sql-select (str sql-select-all " where key = ?"))

(def sql-update (str "update " table-name " set vals = ? where key = ?"))

(def sql-insert (str "insert into " table-name " (key, vals) values (?, ?)"))

(deftype SqlAccessor []
  Accessor
  ;
  (read! [this ignite txn [opcode k v]]
    (let [r (with-open [rs (run-sql (.sql ignite) txn sql-select [k])]
              (let [s (if (.hasNext rs) (.stringValue (.next rs) 1) "")]
                (as-int-list s)))]
      [:r k r]))
  ;
  (append! [this ignite txn [opcode k v]]
    (with-open [read-rs (run-sql (.sql ignite) txn sql-select [k])]
      (if (.hasNext read-rs)
        ; update existing list
        (let [old-list    (.stringValue (.next read-rs) 1)
              new-list    (str old-list "," v)]
          (with-open [write-rs (run-sql (.sql ignite) txn sql-update [new-list k])]))
        ; create a new list
        (with-open [write-rs (run-sql (.sql ignite) txn sql-insert [k (str v)])]))
      [opcode k v])))

; ---------- KV Access ----------

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

; ---------- General scenario ----------

(defn extract-reason [exc]
  "Convert an exception into a known reason, or return nil."
  (let [msg (.getMessage exc)]
    (cond
        (.contains msg "Failed to acquire a lock") ::deadlock-prevention
        (or (.contains msg "Failed to process replica request")
            (.contains msg "Node left the cluster")
            (.contains msg "The primary replica has changed")
            (.contains msg "Unable to request next batch")
            (.contains msg "Unable to send fragment")) ::not-connected
        :else nil)))

(defn fail [op error]
  "Mark operation op as failed with a given error."
  (assoc op :type :fail :error error))

(defn invoke-ops [^Ignite ignite acc op]
  "Perform operations in a transaction."
  (try
    (let [txn (.begin (.transactions ignite))
          result (mapv #(case (first %)
                          :r       (read! acc ignite txn %)
                          :append  (append! acc ignite txn %))
                       (:value op))]
      (.commit txn)
      (assoc op :type :info :value (into [] result)))
    (catch SqlException e
      (if-some [reason (extract-reason e)] (fail op reason) (throw e)))
    (catch TransactionException e
      (if-some [reason (extract-reason e)] (fail op reason) (throw e)))
    (catch IgniteClientConnectionException _
      (fail op ::not-connected))))

(defn print-table-content [ignite]
  "Save resulting table content in the log."
  (try
    (with-open [rs (run-sql (.sql ignite) sql-select-all [])]
      (log/info "Table content")
      (while (.hasNext rs)
        (let [row (.next rs)]
          (log/info (.intValue row 0) ":" (.stringValue row 1)))))
    (catch Exception e
      (log/warn "Failed to get table content:" (.getMessage e)))))

(defrecord Client [^Ignite ignite acc connected]
  client/Client

  (open! [this test node]
    (try
      (let [ignite (-> (IgniteClient/builder)
                       (.addresses (into-array [(str node ":10800")]))
                       (.retryPolicy (RetryLimitPolicy.))
                       (.build))]
        (assoc this :ignite ignite, :connected true))
      (catch IgniteClientConnectionException _
        (assoc this :connected false))))

  (close! [this test]
    (when connected
      (.close ignite)))

  (setup! [this test]
    (when connected
      (with-open [create-zone-stmt    (.createStatement (.sql ignite)
                                                        (sql-create-zone (count (:nodes test))))
                  zone-rs             (run-sql (.sql ignite) create-zone-stmt [])
                  create-table-stmt   (.createStatement (.sql ignite) sql-create)
                  table-rs            (run-sql (.sql ignite) create-table-stmt [])]
        (log/info "Table" table-name "created"))))

  (teardown! [this test]
    (when connected
      (print-table-content ignite)))

  (invoke! [this test op]
    (if connected
      (invoke-ops ignite acc op)
      (fail op ::not-connected))))

(comment "for repl"

(def c (client/open! (Client. nil (KeyValueAccessor.) false) {} "127.0.0.1"))
(client/setup! c {})

(client/invoke! c {} {:type :invoke, :process 0, :f :txn, :value [[:r 5 nil] [:r 6 nil]]})

(client/invoke! c {} {:type :invoke, :process 1, :f :txn, :value [[:append 9 2]]})

(client/invoke! c {} {:type :invoke, :process 0, :f :txn, :value [[:r 9 nil]]})

(client/teardown! c {})

(client/close! c {})

"/for repl"
)

(def accessors
  {"sql" (SqlAccessor.)
   "kv"  (KeyValueAccessor.)})

(defn append-test
  [opts]
  (ignite3/basic-test
    (merge
      (let [test-ops {:consistency-models [:strict-serializable]}]
        {:name      "append-test"
         :client    (Client. nil (get accessors (:accessor opts)) false)
         :checker   (app/checker test-ops)
         :generator (ignite3/wrap-generator (app/gen test-ops) (:time-limit opts))})
      opts)))
