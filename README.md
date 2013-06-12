# clj-hdfs

Small lib to make it easier to work with data on Hadoop's Distributed File System (HDFS).

## Usage

More examples are in the tests. It's still early days but reading and writing with SequenceFiles is implemented.

```clj
(use 'clj-hdfs.core)
(import org.apache.hadoop.io.LongWritable)

(def config (create-configuration {}))

(let [tmp-path (path "./tmp/appender.seq")]
  (with-open [writer (create-sequence-writer config tmp-path LongWritable LongWritable)]
    (let [append (appender writer (LongWritable. ) (LongWritable. ))]
      (append {130 110})))
  (with-open [reader (create-sequence-reader config tmp-path)]
    (let [record (first (reader-seq reader (LongWritable.) (LongWritable.)))]
      (println record))))
;; {130 110}
```

### Compression

You can specify the compression type and compression codec you'd like to create the SequenceFile with too.

```clj
(use 'clj-hdfs.core)
(import org.apache.hadoop.io.LongWritable)

(def config (create-configuration {}))

(let [tmp-path (path "./tmp/appender.seq")]
  (with-open [writer (create-sequence-writer config tmp-path LongWritable LongWritable :compression-type :block :compression-codec :snappy)]
    (let [append (appender writer (LongWritable. ) (LongWritable. ))]
      (append {130 110})))
  (with-open [reader (create-sequence-reader config tmp-path)]
    (let [record (first (reader-seq reader (LongWritable.) (LongWritable.)))]
      (println record))))
;; {130 110}
```

## License

Copyright &copy; 2012 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
