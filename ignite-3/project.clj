(defproject jepsen.ignite-3 "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Apache Ignite 3"
  :url "https://ignite.apache.org/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.4"]
                 [org.apache.ignite/ignite-client "3.0.0-SNAPSHOT"]
                 [org.apache.ignite/ignite-core "3.0.0-SNAPSHOT"]]
  :profiles {:gg {:repositories
                    [["gridgain-snapshots" "https://www.gridgainsystems.com/nexus/content/repositories/gridgain-snapshots/"]]}}
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :main jepsen.ignite3.runner
  :aot [jepsen.ignite3.runner])
