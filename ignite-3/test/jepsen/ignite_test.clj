(ns jepsen.ignite-test
  (:require [clojure.test :refer :all]
            [jepsen.ignite3 :refer :all]))

(deftest node-name-test
  (testing "Mapping of node addresses into readable node names."
    (let [nodes ["192.168.1.5", "192.168.1.6", "192.168.1.4"]]
      (is (= "node-1" (node-name nodes "192.168.1.5")))
      (is (= "node-2" (node-name nodes "192.168.1.6")))
      (is (= "node-3" (node-name nodes "192.168.1.4")))
      ; probably, should throw an exception instead
      (is (= "node-0" (node-name nodes "192.168.1.7"))))))

(deftest init-command-test
  (testing "Generation of init command sequence"
    (let [test1 {:nodes ["n1"]}
          test2 {:nodes ["n1" "n2"]}
          test3 {:nodes ["n1" "n2" "n3"]}
          test4 {:nodes ["n1" "n2" "n3" "n4"]}
          test5 {:nodes ["n1" "n2" "n3" "n4" "n5"]}]
      (is (= ["--cluster-name=ignite-cluster" "--meta-storage-node" "node-1"]
             (init-command test1)))
      (is (= ["--cluster-name=ignite-cluster" "--meta-storage-node" "node-1"]
             (init-command test2)))
      (is (= ["--cluster-name=ignite-cluster" "--meta-storage-node" "node-1"]
             (init-command test3)))
      (is (= ["--cluster-name=ignite-cluster" "--meta-storage-node" "node-1"
                                              "--meta-storage-node" "node-2"
                                              "--meta-storage-node" "node-3"]
             (init-command test4)))
      (is (= ["--cluster-name=ignite-cluster" "--meta-storage-node" "node-1"
                                              "--meta-storage-node" "node-2"
                                              "--meta-storage-node" "node-3"]
             (init-command test5))))))
