(ns jepsen.ignite3.register
  "Single atomic register test"
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [ignite3 :as ignite3]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]))

(defn r
  "read operation"
  [_ _] {:type :invoke, :f :read, :value nil})

(defn w
  "write operation"
  [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn cas
  "compare and set operation"
  [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client []
  client/Client
  (open! [this test node]))

(defn test
  [opts]
  (ignite3/basic-test
    (merge
      {:name      "register-test"
       :client    (Client.)
       :checker   (independent/checker
                    (checker/compose
                      {:linearizable (checker/linearizable {:model (model/cas-register)})
                       :timeline  (timeline/html)}))
       :generator (ignite3/generator [r w cas] (:time-limit opts))}
      opts)))