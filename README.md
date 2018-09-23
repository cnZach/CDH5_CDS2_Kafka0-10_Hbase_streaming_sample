# spark2-use-hbase-api-cdh5

this is an example streaming from kafka, then use standard hbase java client and hadoop UGI within CDH spark2. The UGI login code is referred to Stack's original work: github[link:https://github.com/saintstack/hbase-downstreamer/blob/master/hbase-1/src/main/java/org/hbase/downstreamer/spark/JavaNetworkWordCountStoreInHBase.java#L130]

this sample build with spark2.1 Cloudera2 and CDH 5.7.5.

1) prepare the hbase jars for classpath:
{% highlight bash %}
export HBASE_JAR=/opt/cloudera/parcels/CDH/jars/hbase-annotations-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-client-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-common-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-examples-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-external-blockcache-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-hadoop-compat-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-hadoop2-compat-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-it-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-prefix-tree-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-procedure-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-protocol-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-resource-bundle-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-rest-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-rsgroup-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-server-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-shell-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH/jars/hbase-thrift-1.2.0-cdh5.7.5.jar:/opt/cloudera/parcels/CDH-5.7.5-1.cdh5.7.5.p0.3/lib/hbase/lib/htrace-core.jar
{% endhighlight %}

2) run spark2-suibmit with 08:
{% highlight bash %}
spark2-submit --class org.hbase.myexample.spark.JavaKafka08WordCountStoreInHBase --files my_log4j.conf --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/java/jdk1.8.0_60 --conf spark.executorEnv.JAVA_HOME=/usr/java/jdk1.8.0_60 --master yarn --deploy-mode cluster --principal "systest@HADOOP.EXAMPLE.COM" --keytab "systest.keytab" --conf spark.yarn.security.credentials.hbase.enabled=true  --conf spark.executor.extraClassPath=/etc/hbase/conf:${HBASE_JAR} --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=my_log4j.conf"  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=my_log4j.conf" --conf spark.driver.extraClassPath=/etc/hbase/conf:${HBASE_JAR}  --conf spark.dynamicAllocation.enabled=false sparkexample-hbase-api-1.x-1.0-SNAPSHOT.jar 10.17.103.100:9092 test_log
{% endhighlight %}
