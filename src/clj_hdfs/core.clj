(ns clj-hdfs.core
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.io SequenceFile SequenceFile$Reader]))

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

(defn create-sequence-reader
  [conf path]
  (SequenceFile$Reader. (.getFileSystem path conf) path conf))

(defn reader-seq
  [rdr key val]  
  (lazy-seq
   (loop [xs '()]
     (let [more? (.next rdr key val)]
       (if (not more?)
         xs
         (recur (cons {(.get key) (.get val)} xs)))))))