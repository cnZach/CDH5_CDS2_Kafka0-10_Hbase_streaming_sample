# spark2-use-hbase-api-cdh5

This is an example streaming from secured kafka, then use standard hbase java client and hadoop UGI within CDH spark2. The UGI login code is referred to Stack's original work: [github](https://github.com/saintstack/hbase-downstreamer/blob/master/hbase-1/src/main/java/org/hbase/downstreamer/spark/JavaNetworkWordCountStoreInHBase.java#L130)

This sample project builds with spark2.4 Cloudera2 and CDH 5.14.4, but it should aslo work with other CDS2.x versions as well.

# How to run it:

1) Prepare keytab and jaas file for the spark application
Sample jaas file:
```
cat kafka_jaas.conf
KafkaClient{
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="./kafka_systest.keytab"
principal="systest@ZYX.COM";

};
Client{
com.sun.security.auth.module.Krb5LoginModule required
useTicketCache=true;
};
```

2) create 'test_counts' table in hbase

```
Version 1.2.0-cdh5.14.4, rUnknown, Tue Jun 12 04:00:36 PDT 2018

hbase(main):002:0> create 'test_counts','word_counts'
```

3) build and run this app with kafka-0-10 integration:
run in yarn-client mode:
```
 SPARK_KAFKA_VERSION=0.10 spark2-submit --master yarn --deploy-mode client \
 --class org.hbase.myexample.spark.JavaKafka10WordCountStoreInHBase \
 --files kafka_jaas.conf --keytab systest.keytab --principal systest@ZYX.COM \
 --conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=kafka_jaas.conf" \
 --driver-java-options "-Djava.security.auth.login.config=kafka_jaas.conf" \
 spark2example-kafka010-hbase-api-1.x-1.0-SNAPSHOT.jar 10.17.101.127:9093 cdk140 SASL_PLAINTEXT test-group
```

or in yarn-cluster mode:

```
SPARK_KAFKA_VERSION=0.10 spark2-submit --master yarn --deploy-mode cluster \
--class org.hbase.myexample.spark.JavaKafka10WordCountStoreInHBase \
--files kafka_systest.keytab,kafka_jaas.conf --keytab systest.keytab --principal systest@ZYX.COM \
--conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=kafka_jaas.conf" \
--conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=kafka_jaas.conf" \
spark2example-kafka010-hbase-api-1.x-1.0-SNAPSHOT.jar 10.17.101.127:9093 cdk140 SASL_PLAINTEXT test-group
```

Usage: JavaKafka10WordCountStoreInHBase broker-list topic kafka_security_protocol groupId remote-hbase-site.xml


# What does the app do:

 The app Counts words in UTF8 encoded, '\n' delimited text received from the network every second. Then
  stores the counts in HBase 'test_counts' table with a layout of:

  RowID          |     CF          |   CQ     | value
  time in millis  |  "word_counts"  |  <word>  |  <count for period>
  
  Sample result in hbase :
  ```
  hbase(main):014:0> scan 'test_counts',{'LIMIT'=>1}
  ROW                                                        COLUMN+CELL
   1560335060000 ms                                          column=word_counts:, timestamp=1560335062545, value=\x00\x00\x00\x01
   1560335060000 ms                                          column=word_counts:=new=====_, timestamp=1560335062598, value=\x00\x00\x00\x01
   1560335060000 ms                                          column=word_counts:OK, timestamp=1560335062545, value=\x00\x00\x00\x01
   1560335060000 ms                                          column=word_counts:xmf, timestamp=1560335062598, value=\x00\x00\x00\x01
  1 row(s) in 0.1300 seconds
  ```

  This app works on secured clusters for an indefinite period via keytab login. NOTE: copies the given keytab to the working directory of executors.


