(ns clj-hdfs.test.core
  (:import [java.io File]
           [org.apache.hadoop.io LongWritable])
  (:use [clojure.test]
        [clj-hdfs.serialize :only (writable)]
        [clj-hdfs.core :only (path reader-seq create-sequence-writer appender create-sequence-reader create-configuration filesystem exists? filesystem delete list-statuses mkdirs rename)] :reload))

(def config (create-configuration {}))

(deftest sequence-file-reading-and-writing
  (let [tmp-path "./tmp/test.seq"
        key-class LongWritable
        val-class LongWritable]
    (with-open [writer (create-sequence-writer config (path tmp-path) key-class val-class)])
    (is (true? (.exists (File. tmp-path))))

    (with-open [writer (create-sequence-writer config (path tmp-path) key-class val-class)]
      (.append writer (writable 30) (writable 10)))
    (with-open [reader (create-sequence-reader config (path tmp-path))]
      (let [records (reader-seq reader (LongWritable.) (LongWritable.))
            record (first records)]
        (is (seq? records))
        (is (= 1 (count records)))
        (is (= {30 10} record)))) 
    (let [fs (filesystem config)
          p (path "./tmp/test.seq")]
      (is (true? (exists? fs p)))
      (delete fs p)
      (is (false? (exists? fs p))))))

(deftest directories
  (let [fs (filesystem config)
        p (path "./tmp/test-dir")
        p2 (path "./tmp/test-dir-2")]
    (delete fs p)
    (is (false? (exists? fs p)))
    (mkdirs fs p)
    (is (true? (exists? fs p)))
    (rename fs p p2)
    (is (false? (exists? fs p)))
    (is (true? (exists? fs p2)))))

(deftest sequence-file-appending
  (let [tmp-path "./tmp/appender.seq"]
    (with-open [writer (create-sequence-writer config (path tmp-path) LongWritable LongWritable)]
      (let [append (appender writer (LongWritable. ) (LongWritable. ))]
        (append {130 110})))
    (with-open [reader (create-sequence-reader config (path tmp-path))]
      (let [record (first (reader-seq reader (LongWritable.) (LongWritable.)))]
        (is (= {130 110} record))))))

(deftest files-exist
  (let [fs (filesystem config)
        p (path "./tmp/exists.seq")]
    (with-open [writer (create-sequence-writer config p LongWritable LongWritable)]
      (is (exists? fs p)))))

