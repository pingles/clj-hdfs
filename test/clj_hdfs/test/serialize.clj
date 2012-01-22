(ns clj-hdfs.test.serialize
  (:import [org.apache.hadoop.io LongWritable])
  (:use [clojure.test]
        [clj-hdfs.serialize] :reload))

(deftest writable-protocol
  (is (= 30 (.get (writable 30))))
  (let [w (LongWritable. )]
    (set-writable w 100)
    (is (= 100 (.get w)))))