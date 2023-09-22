(defproject jepsen.ignite-3 "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Apache Ignite 3"
  :url "https://ignite.apache.org/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3"]
                 [org.apache.ignite/ignite-core "3.0.0-SNAPSHOT"]]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :main jepsen.ignite3.runner
  :aot [jepsen.ignite3.runner])
