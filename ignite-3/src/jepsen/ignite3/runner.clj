(ns jepsen.ignite3.runner
  "Runs Apache Ignite 3 tests."
  (:gen-class)
  (:require [clojure.pprint             :refer [pprint]]
            [clojure.tools.logging      :refer :all]
            [jepsen.cli                 :as jc]
            [jepsen.core                :as jepsen]
            [jepsen.ignite3.append      :as append]
            [jepsen.ignite3.register    :as register]))

(def tests
  "A map of test names to test constructors."
  {"append"     append/append-test
   "register"   register/register-test})

(def nemesis-types
  {"noop"                   jepsen.nemesis/noop})

(def opt-spec
  "Command line options for tools.cli"
  [(jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)
   ["-v" "--version VERSION"
    "What version of Apache Ignite to install"
    :default "3.0.0-SNAPSHOT"]
   ["-f" "--flavour FLAVOUR"
    "What flavour of product to install"
    :default "ignite3"]
   ["-o" "--os NAME" "Operating system: either centos, debian, or noop."
    :default  :noop
    :parse-fn keyword
    :validate [#{:centos :debian :noop} "One of `centos` or `debian` or 'noop'"]]
   [nil "--url URL" "URL to Ignite zip to install, has precedence over --version"
    :default nil
    :parse-fn str]
   ["-a" "--accessor ACCESSOR" (str "Accesor type, for append test only " (keys append/accessors))
    :default (first (keys append/accessors))
    :validate [(set (keys append/accessors)) (jc/one-of append/accessors)]]
   ["-nemesis" "--nemesis Nemesis"
    "What Nemesis to use"
    :default jepsen.nemesis/noop
    :parse-fn nemesis-types
    :validate [identity (jc/one-of nemesis-types)]]])

(defn log-test
  [t]
  (binding [*print-length* 100] (info "Testing\n" (with-out-str (pprint t))))
  t)

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/test-opt-fn
                         (jc/rename-options {
                                             :test :test-fns})))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (doseq [i        (range (:test-count options))
                          test-fn  (:test-fns options)]
                    ; Rehydrate test and run
                    (let [
                          test (-> options
                                   test-fn
                                   log-test
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
