# spark2-use-hbase-api-cdh5

this is an example streaming from secured kafka, then use standard hbase java client and hadoop UGI within CDH spark2. The UGI login code is referred to Stack's original work: [github](https://github.com/saintstack/hbase-downstreamer/blob/master/hbase-1/src/main/java/org/hbase/downstreamer/spark/JavaNetworkWordCountStoreInHBase.java#L130)

this sample build with spark2.4 Cloudera2 and CDH 5.14.4.

run spark2-suibmit with kafka-0-10 in yarn-client mode:
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


 the app Counts words in UTF8 encoded, '\n' delimited text received from the network every second. Then
  stores the counts in HBase 'test_counts' table with a layout of:

  RowID          |     CF          |   CQ     | value
  time in millis  |  "word_counts"  |  <word>  |  <count for period>

  Works on secure clusters for an indefinite period via keytab login. NOTE: copies the given keytab to the working directory of executors.

  Usage: JavaKafka10WordCountStoreInHBase broker-list topic kafka_security_protocol groupId

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
