(ns clj-hdfs.core
  (:import [java.util Date]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.fs Path FileSystem FileStatus]
           [org.apache.hadoop.io SequenceFile SequenceFile$Reader SequenceFile$CompressionType]
           [org.apache.hadoop.io.compress DefaultCodec GzipCodec SnappyCodec])
  (:use [clj-hdfs.serialize]
        [clojure.string :only (join)]))

(defn create-configuration
  [m]
  (let [c (Configuration. )]
    (doseq [[n v] m] (.set c n v))
    c))

(defn path
  "Allows a number of path parts to be specified.
   example: (path \"/user\" \"name\")  => (Path. \"/user/name\")"
  [& parts]
  (Path. (join "/" parts)))

(defn exists?
  [fs path]
  (.exists fs path))

(defn delete
  "Deletes a path. Defaults to a recursive delete if path
   is a directory."
  [fs path & {:keys [recursive] :or {recursive true}}]
  (.delete fs path recursive))

(defn mkdirs
  [fs path]
  (.mkdirs fs path))

(defn open
  "Opens an InputStream to the specified path."
  [fs path]
  (.open fs path))

(defn rename
  [fs src dst]
  (.rename fs src dst))

(defn filesystem
  [config]
  (FileSystem/get config))

(defn list-statuses
  "Retrieves statuses for a path: files and directories
   contained within."
  [fs path]
  (map (fn [x] {:path (.getPath x)
               :length (.getLen x)
               :is-dir (.isDir x)
               :modified (Date. (.getModificationTime x))
               :replication (.getReplication x)})
       (.listStatus fs path)))

(def compression-types
  {:block SequenceFile$CompressionType/BLOCK
   :none SequenceFile$CompressionType/NONE
   :record SequenceFile$CompressionType/RECORD})

(defn compression-codecs
  [t]
  (condp = t
    :gzip (GzipCodec. )
    :snappy (SnappyCodec. )
    (DefaultCodec. )))

(defn create-sequence-writer
  [conf path key-class val-class & {:keys [compression-type compression-codec]
                                    :or   {compression-type :block
                                           compression-codec :default}}]
  (SequenceFile/createWriter (.getFileSystem path conf)
                             conf
                             path
                             key-class
                             val-class
                             (compression-types compression-type)
                             (compression-codecs compression-codec)))

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
  "Returns a lazy sequence of maps representing data
   within a sequence file."
  [rdr key val]  
  (lazy-seq
   (loop [xs '()]
     (let [more? (.next rdr key val)]
       (if (not more?)
         xs
         (recur (cons {(.get key) (.get val)} xs)))))))
