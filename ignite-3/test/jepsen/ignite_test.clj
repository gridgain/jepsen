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
    (let [test1 {:flavour "ignite3" :nodes ["n1"]}
          test2 {:flavour "ignite3" :nodes ["n1" "n2"]}
          test3 {:flavour "ignite3" :nodes ["n1" "n2" "n3"]}
          test4 {:flavour "ignite3" :nodes ["n1" "n2" "n3" "n4"]}
          test5 {:flavour "ignite3" :nodes ["n1" "n2" "n3" "n4" "n5"]}]
      (is (= ["bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1"]
             (init-command test1)))
      (is (= ["bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1"]
             (init-command test2)))
      (is (= ["bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1,node-2,node-3"]
             (init-command test3)))
      (is (= ["bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1,node-2,node-3"]
             (init-command test4)))
      (is (= ["bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1,node-2,node-3"]
             (init-command test5)))))

  (testing "Use of extra init options"
    (let [test1 {:flavour               "ignite3"
                 :nodes                 ["n1" "n2" "n3"]
                 :extra-init-options    "--config-files=my.conf"}]
      (is (= ["bin/ignite3" "cluster" "init" "--config-files=my.conf" "--name=ignite-cluster" "--metastorage-group=node-1,node-2,node-3"]
             (init-command test1)))))

  (testing "Pass custom environment"
    (let [test1 {:flavour       "gridgain9"
                 :nodes         ["n1"]
                 :environment  "JAVA_HOME=/opt/java/jdk-open-11"}
          test2 {:flavour       "ignite3"
                 :nodes         ["n1"]
                 :environment   "JAVA_HOME=/opt/java/jdk-open-17"}]
      (is (= [:env "JAVA_HOME=/opt/java/jdk-open-11" "bin/gridgain9" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1"]
             (init-command test1)))
      (is (= [:env "JAVA_HOME=/opt/java/jdk-open-17" "bin/ignite3" "cluster" "init" "--name=ignite-cluster" "--metastorage-group=node-1"]
             (init-command test2))))))
