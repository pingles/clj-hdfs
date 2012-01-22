(ns clj-hdfs.core
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.io SequenceFile SequenceFile$Reader])
  (:use [clj-hdfs.serialize]))

(defn create-configuration
  [m]
  (let [c (Configuration. )]
    (doseq [[n v] m] (.set c n v))
    c))

(defn path
  [str]
  (Path. str))

(defn exists?
  [fs path]
  (.exists fs path))

(defn delete
  "Deletes a path. Defaults to a recursive delete if path
   is a directory."
  [fs path & {:keys [recursive] :or {recursive true}}]
  (.delete fs path recursive))

(defn filesystem
  [config]
  (FileSystem/get config))

(defn create-sequence-writer
  [conf path key-class val-class]
  (SequenceFile/createWriter (.getFileSystem path conf)
                             conf
                             path
                             key-class
                             val-class))

(defn appender
  "Creates a fn that accepts a single argument representing
   a sequence of key/vals to append to the specified Writer. 
   Uses the Writable protocol's set-writable to update
   the instance values."
  [writer key val]
  (fn [m]
    (doseq [[k v] m]
      (set-writable key k)
      (set-writable val v)
      (.append writer key val))))

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