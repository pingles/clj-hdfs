(ns clj-hdfs.core
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.io SequenceFile]))

(defn create-configuration
  [m]
  (let [c (Configuration. )]
    (doseq [[n v] m] (.set c n v))
    c))

(defn path
  "Creates a Path from the specified path."
  [str]
  (Path. str))

(defn create-sequence-writer
  [conf path key-class val-class]
  (SequenceFile/createWriter (.getFileSystem path conf)
                             conf
                             path
                             key-class
                             val-class))