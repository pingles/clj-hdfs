(ns clj-hdfs.core
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.fs Path]))

(defn create-configuration
  [m]
  (let [c (Configuration. )]
    (doseq [[n v] m] (.set c n v))
    c))

(defn create-job-conf
  [m]
  (JobConf. (configuration m)))


(defn path
  "Creates a Path from the specified path."
  [str]
  (Path. str))