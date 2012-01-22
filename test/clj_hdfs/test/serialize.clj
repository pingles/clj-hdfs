(ns clj-hdfs.test.serialize
  (:import [org.apache.hadoop.io BytesWritable DoubleWritable LongWritable])
  (:use [clojure.test]
        [clj-hdfs.serialize] :reload))

(deftest writable-protocol
  (testing "Longs"
    (is (= 30 (.get (writable 30))))
    (let [w (LongWritable. )]
      (set-writable w 100)
      (is (= 100 (.get w)))))

  (testing "Doubles"
    (is (= (writable (double 50)) (DoubleWritable. (double 50))))
    (let [w (DoubleWritable. )]
      (set-writable w 50)
      (is (= (double 50)) (.get w))))

  (testing "Bytes"
    (let [bytes (map byte [1 2 3 4 5])
          w (BytesWritable. )]
      (set-writable w bytes)
      (is (= 5 (.getSize w)))
      (is (= bytes
             (take 5 (.get w)))))))