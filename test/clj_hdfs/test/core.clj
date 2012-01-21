(ns clj-hdfs.test.core
  (:import [java.io File]
           [org.apache.hadoop.io LongWritable])
  (:use [clojure.test]
        [clj-hdfs.core :only (path create-sequence-writer create-configuration)] :reload))

(def config (create-configuration {}))

(deftest sequence-file-writing
  (let [tmp-path "./tmp/test.seq"
        key-class LongWritable
        val-class LongWritable]
    (with-open [writer (create-sequence-writer config (path tmp-path) key-class val-class)])
    (is (true? (.exists (File. tmp-path))))))