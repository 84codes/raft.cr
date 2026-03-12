(defproject jepsen.raft-kv "0.1.0"
  :description "Jepsen tests for Raft KV store"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.13.0"]]
  :main jepsen.raft-kv
  :repl-options {:init-ns jepsen.raft-kv}
  :jvm-opts ["-Djava.awt.headless=true"])
