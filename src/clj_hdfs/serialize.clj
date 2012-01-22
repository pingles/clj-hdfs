(ns clj-hdfs.serialize
  (:import [org.apache.hadoop.io BytesWritable DoubleWritable LongWritable Text FloatWritable IntWritable]))

(defprotocol ToWritable
  (writable [x] "Creates a new instance of a Writable class")
  (set-writable [w x] "Sets writable w to value x"))

(extend-protocol ToWritable
  Long
  (writable [x] (LongWritable. x))
  Double
  (writable [x] (DoubleWritable. x))
  String
  (writable [x] (Text. x))
  Float
  (writable [x] (FloatWritable. x))
  Integer
  (writable [x] (IntWritable. x))
  
  LongWritable
  (set-writable [w x] (.set w x))
  DoubleWritable
  (set-writable [w x] (.set w x))
  BytesWritable
  (set-writable [w xs] (.set w (byte-array xs) 0 (count xs)))
  FloatWritable
  (set-writable [w x] (.set w x))
  IntWritable
  (set-writable [w x] (.set w x))
  Text
  (set-writable [w x] (.set w x)))
