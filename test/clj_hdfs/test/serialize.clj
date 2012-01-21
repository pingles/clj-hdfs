(ns clj-hdfs.test.serialize
  (:use [clojure.test]
        [clj-hdfs.serialize] :reload))

(deftest writable-protocol
  (is (= 30 (.get (writable 30)))))