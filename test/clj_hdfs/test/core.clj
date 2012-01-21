(ns clj-hdfs.test.core
  (:import [java.io File]
           [org.apache.hadoop.io LongWritable])
  (:use [clojure.test]
        [clj-hdfs.serialize :only (writable)]
        [clj-hdfs.core :only (path create-sequence-writer create-sequence-reader create-configuration)] :reload))

(def config (create-configuration {}))

(deftest sequence-file-writing
  (let [tmp-path "./tmp/test.seq"
        key-class LongWritable
        val-class LongWritable]
    (with-open [writer (create-sequence-writer config (path tmp-path) key-class val-class)])
    (is (true? (.exists (File. tmp-path))))

    (with-open [writer (create-sequence-writer config (path tmp-path) key-class val-class)]
      (.append writer (writable 30) (writable 10)))
    (with-open [reader (create-sequence-reader config (path tmp-path))]
      (let [key (LongWritable. )
            val (LongWritable. )]
        (.next reader key val)
        (is (= 30 (.get key)))
        (is (= 10 (.get val)))))))

