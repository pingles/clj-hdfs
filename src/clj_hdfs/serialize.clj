(ns clj-hdfs.serialize
  (:import [org.apache.hadoop.io LongWritable]))

(defprotocol ToWritable
  (writable [x] "Creates a new instance of a Writable class"))

(extend-protocol ToWritable
  Long
  (writable [x] (LongWritable. x)))
