(ns jepsen.raft-kv
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [knossos.model :as model]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+]]))

(defn node-id
  "Extract numeric ID from node name like 'n1' -> 1"
  [node]
  (Long/parseLong (re-find #"\d+" (name node))))

(defn peer-string
  "Build peer string for a node, excluding itself"
  [test node]
  (->> (:nodes test)
       (remove #{node})
       (map #(str (name %) ":9000"))
       (str/join ",")))

(defn db
  "Raft KV database"
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up KV node on" node)
      (c/su
        (c/exec :mkdir :-p "/data/raft")
        ;; Write a startup script with environment variables
        (c/exec :bash :-c
                (str "cat > /usr/local/bin/start-kv.sh << 'SCRIPT'\n"
                     "#!/bin/bash\n"
                     "export NODE_ID=" (node-id node) "\n"
                     "export PEERS='" (peer-string test node) "'\n"
                     "export HTTP_PORT=8080\n"
                     "export RAFT_PORT=9000\n"
                     "export DATA_DIR=/data/raft\n"
                     "exec /usr/local/bin/kv_node\n"
                     "SCRIPT\n"
                     "chmod +x /usr/local/bin/start-kv.sh"))
        (cu/start-daemon!
          {:logfile "/var/log/kv_node.log"
           :pidfile "/var/run/kv_node.pid"
           :chdir   "/"}
          "/usr/local/bin/start-kv.sh")
        (Thread/sleep 3000)))

    (teardown! [_ test node]
      (info "Tearing down KV node on" node)
      (c/su
        (cu/stop-daemon! "/usr/local/bin/kv_node" "/var/run/kv_node.pid")
        (c/exec :rm :-rf "/data/raft")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/kv_node.log"])))

(defn try-nodes
  "Try an HTTP request on the preferred node first. If 503 (not leader),
   try other nodes in random order."
  [test preferred-node request-fn]
  (let [resp (try (request-fn preferred-node)
                  (catch Exception e {:status -1 :error e}))]
    (if (= 503 (:status resp))
      (let [others (shuffle (remove #{preferred-node} (:nodes test)))]
        (reduce (fn [last-resp node]
                  (let [r (try (request-fn node)
                               (catch Exception e {:status -1 :error e}))]
                    (if (= 503 (:status r))
                      r
                      (reduced r))))
                resp
                others))
      resp)))

(defn kv-url [node key]
  (str "http://" (name node) ":8080/kv/" key))

(defrecord Client [node]
  client/Client
  (open! [this test node]
    (assoc this :node node))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k v] (:value op)
          kname (str "key-" k)]
      (try+
        (case (:f op)
          :read
          (let [resp (try-nodes test node
                       #(http/get (kv-url % kname)
                                  {:throw-exceptions false
                                   :socket-timeout    5000
                                   :connection-timeout 5000}))]
            (cond
              (= -1 (:status resp))
              (assoc op :type :fail :error :connection-error)

              (= 200 (:status resp))
              (let [body (json/parse-string (:body resp) true)
                    val  (when (:value body)
                           (try (Long/parseLong (:value body))
                                (catch NumberFormatException _ (:value body))))]
                (assoc op :type :ok :value (independent/tuple k val)))

              (= 404 (:status resp))
              (assoc op :type :ok :value (independent/tuple k nil))

              :else
              (assoc op :type :fail :error [:status (:status resp)])))

          :write
          (let [resp (try-nodes test node
                       #(http/put (kv-url % kname)
                                  {:body               (str v)
                                   :throw-exceptions   false
                                   :socket-timeout     10000
                                   :connection-timeout 5000
                                   :query-params       {"wait" "true"}}))]
            (cond
              (= -1 (:status resp))
              (assoc op :type :info :error :connection-error)

              (#{200 201} (:status resp))
              (assoc op :type :ok)

              (= 202 (:status resp))
              ;; Accepted but not confirmed committed
              (assoc op :type :ok)

              (= 408 (:status resp))
              (assoc op :type :info :error :commit-timeout)

              (= 503 (:status resp))
              ;; All nodes returned not-leader (possible during partition)
              (assoc op :type :info :error :no-leader)

              :else
              (assoc op :type :info :error [:status (:status resp)]))))

        (catch java.net.SocketTimeoutException _
          (assoc op :type (if (= :read (:f op)) :fail :info)
                    :error :socket-timeout))
        (catch java.net.ConnectException _
          (assoc op :type (if (= :read (:f op)) :fail :info)
                    :error :connection-refused))
        (catch Exception e
          (assoc op :type (if (= :read (:f op)) :fail :info)
                    :error (.getMessage e))))))

  (teardown! [this test])
  (close! [_ test]))

(defn raft-kv-test
  "Given CLI options, construct a test map"
  [opts]
  (merge tests/noop-test
         opts
         {:name      "raft-kv"
          :os        jepsen.os/noop
          :db        (db)
          :client    (Client. nil)
          :nemesis   (nemesis/partition-random-halves)
          :ssh       {:username                 "root"
                      :private-key-path         "/root/.ssh/id_rsa"
                      :strict-host-key-checking false}
          :checker   (checker/compose
                       {:perf     (checker/perf)
                        :timeline (timeline/html)
                        :linear   (independent/checker
                                    (checker/linearizable
                                      {:model     (model/register nil)
                                       :algorithm :linear}))})
          :generator (->> (independent/concurrent-generator
                            5       ; threads per key
                            (range) ; infinite key sequence
                            (fn [k]
                              (->> (gen/mix
                                     [(fn [_ _] {:type :invoke :f :read  :value nil})
                                      (fn [_ _] {:type :invoke :f :write :value (rand-int 100)})])
                                   (gen/limit 50))))
                          (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info :f :start}
                                    (gen/sleep 5)
                                    {:type :info :f :stop}]))
                          (gen/time-limit (:time-limit opts 60)))
          :pure-generators true}))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn raft-kv-test})
                   (cli/serve-cmd))
            args))
