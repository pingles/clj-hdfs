(defproject clj-hdfs "0.0.2"
  :description ""
  :dependencies [[org.clojure/clojure "1.5.1"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "1.0.3"]]
                   :jvm-opts ["-D/usr/local/Cellar/hadoop/1.1.2/libexec/lib/native/Linux-amd64-64"]}})
