(ns clj-hdfs.test.serialize
  (:import [org.apache.hadoop.io BytesWritable DoubleWritable FloatWritable LongWritable IntWritable Text])
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
      (set-writable w (byte-array bytes))
      (is (= 5 (.getSize w)))
      (is (= bytes
             (take 5 (.get w))))))

  (testing "Text"
    (is (= (writable "Hello, Paul!") (Text. "Hello, Paul!")))
    (is (= "Hello, Paul!" (.toString (writable "Hello, Paul!"))))
    (let [w (writable "foo")]
      (set-writable w "bar")
      (is (= "bar" (.toString w)))))

  (testing "Floats"
    (is (= (writable (float 30.5)) (FloatWritable. 30.5)))
    (is (= (float 30.5) (.get (writable (float 30.5)))))
    (let [w (writable (float 30.5))]
      (set-writable w (float 99.0))
      (is (= (float 99.0) (.get w)))))

  (testing "Integers"
    (is (= (writable (Integer/valueOf 10)) (IntWritable. 10)))
    (is (= 10 (.get (writable (Integer/valueOf 10)))))
    (let [w (writable (Integer/valueOf 10))]
      (set-writable w 19)
      (is (= 19 (.get w))))))
