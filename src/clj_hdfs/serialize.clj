(ns clj-hdfs.serialize
  (:import [org.apache.hadoop.io LongWritable]))

(defprotocol ToWritable
  (writable [x] "Creates a new instance of a Writable class")
  (set-writable [w x] "Sets writable w to value x"))

(extend-protocol ToWritable
  Long
  (writable [x] (LongWritable. x))
  LongWritable
  (set-writable [w x] (.set w x)))
