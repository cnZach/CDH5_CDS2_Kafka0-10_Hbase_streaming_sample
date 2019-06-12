package org.hbase.myexample.spark;


import scala.Tuple2;
import scala.Tuple3;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.Iterator;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second. Then
 * stores the counts in HBase using a table layout of:
 *
 *  RowID          |     CF          |   CQ     | value
 * time in millis  |  "word_counts"  |  <word>  |  <count for period>
 *
 * Works on secure clusters for an indefinite period via keytab login. NOTE: copies the given
 * keytab to the working directory of executors.
 *
 * Usage: JavaKafka10WordCountStoreInHBase <broker-list> <topic> <kafka security protocol>
 */
public final class JavaKafka10WordCountStoreInHBase {

  private static final Logger LOG = LoggerFactory.getLogger(JavaKafka10WordCountStoreInHBase.class);

  /**
   * Write each word:count pair into hbase, in a row for the given time period.
   */
  public static final class StoreCountsToHBase implements VoidFunction<Iterator<Tuple2<String,Integer>>> {

    private static final TableName COUNTS = TableName.valueOf("test_counts");
    private static final byte[] WORD_COUNTS = Bytes.toBytes("word_counts");

    private static final ConcurrentHashMap<Tuple2<String,String>, Tuple2<UserGroupInformation, Connection>>
            connections = new ConcurrentHashMap<Tuple2<String,String>, Tuple2<UserGroupInformation, Connection>>();

    private byte[] ROW_ID;
    /** (principal, keytab) */
    private final Tuple3<String, String, String> auth;

    public StoreCountsToHBase(final SparkConf sparkConf, String custom_hbase_site_file) {
      auth = new Tuple3<String,String,String>(sparkConf.get("spark.yarn.principal"), sparkConf.get("spark.yarn.keytab"),custom_hbase_site_file);
    }

    public void setTime(Time time) {
      ROW_ID = Bytes.toBytes(time.toString());
    }

    /**
     * Rely on a map of (principal,keytab) => connection to ensure we only keep around one per
     * Classloader.
     */
    private static Tuple2<UserGroupInformation, Connection> ensureConnection(final Tuple3<String, String, String> auth3)
            throws IOException, InterruptedException {
      Tuple2<String, String> auth = new Tuple2<>(auth3._1(), auth3._2());
      String hbase_file = auth3._3();
      Tuple2<UserGroupInformation, Connection> result = connections.get(auth);
      if (result == null) {
        LOG.info("Setting up HBase connection.");
        try {
          final SparkHadoopUtil util = SparkHadoopUtil.get();
          final Configuration conf = HBaseConfiguration.create(util.newConfiguration(new SparkConf()));
          // This work-around for getting hbase client configs requires that you deploy an HBase GATEWAY
          // role on each node that can run a spark executor.

          if (hbase_file.equals("_EMPTY_")) {
            final File clientConfigs = new File("/etc/hbase/conf");
	    LOG.info("=== Adding default hbase conf from /etc/hbase/conf.");
            for (File siteConfig : clientConfigs.listFiles()) {
              if (siteConfig.getName().endsWith(".xml")) {
                LOG.debug("Adding config resource: {}", siteConfig);
                conf.addResource(siteConfig.toURI().toURL());
              }
            }
          } else {
            final File clientConfigs = new File(hbase_file);
            LOG.info("=== Adding custom hbase resource: {}", clientConfigs);
            conf.addResource(clientConfigs.toURI().toURL());
          }
          UserGroupInformation ugi = null;
          Connection connection = null;
          synchronized(UserGroupInformation.class) {
            LOG.debug("setting UGI conf");
            UserGroupInformation.setConfiguration(conf);
            LOG.debug("Ability to read keytab '{}' : {}", auth._2(), (new File(auth._2())).canRead());
            LOG.debug("logging in via UGI and keytab.");
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(auth._1(), auth._2());
            LOG.info("finished login attempt via UGI and keytab. security set? {}", ugi.isSecurityEnabled());
          }
          connection = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
            @Override
            public Connection run() throws IOException {
              return ConnectionFactory.createConnection(conf);
            }
          });
          // Do something with the connection now, to ensure we have valid credentials.
          final Admin admin = connection.getAdmin();
          try {
            final ClusterStatus status = admin.getClusterStatus();
            LOG.info("connection successful: {}", status);
            final Tuple2<UserGroupInformation, Connection> proposed = new Tuple2<UserGroupInformation, Connection>(ugi, connection);
            final Tuple2<UserGroupInformation, Connection> prior = connections.putIfAbsent(auth, proposed);
            if (prior == null) {
              // our proposed connection is valid.
              result = proposed;
            } else {
              // parallel instantiation beat us to completion
              LOG.warn("Discarding extra connection. No need to be concerned, unless this message happens dozens of times.");
              result = prior;
              connection.close();
            }
          } finally {
            admin.close();
          }
        } catch (IOException exception) {
          LOG.error("Failed to connect to hbase. rethrowing; application should fail.", exception);
          throw exception;
        }
      } else {
        LOG.info("HBase connection exists: " + result.toString());
      }
      return result;
    }

    @Override
    public void call(Iterator<Tuple2<String, Integer>> iterator) throws IOException, InterruptedException {
      try {
        Tuple2<UserGroupInformation, Connection> hbase = ensureConnection(auth);
        // This presumes we can complete our Put calculation before the TGT expires.
        // At worst after this check we will have 20% of the ticket window left; for 24hr tickets that means
        // just under 5 hours.
        hbase._1().checkTGTAndReloginFromKeytab();
        final Table table = hbase._2().getTable(COUNTS);
        Put put = new Put(ROW_ID);
        while (iterator.hasNext()) {
          final Tuple2<String, Integer> wordCount = iterator.next();
          put.addColumn(WORD_COUNTS, Bytes.toBytes(wordCount._1), Bytes.toBytes(wordCount._2));
        }
        if (!put.isEmpty()) {
          LOG.debug("Putting " + put.size() + " cells.");
          table.put(put);
        }
      } catch (IOException exception) {
        LOG.error("Failed to put cells. rethrowing; application should fail.", exception);
        throw exception;
      }
    }
  }

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception{
    if (args.length < 4) {
      System.err.println("Usage: JavaKafka10WordCountStoreInHBase <broker-list> <topic> <protocol> <groupId> custom-hbase-site-file-name");
      System.exit(1);
    }

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafka10WordCountStoreInHBase");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    String brokers = args[0];
    String topics = args[1];
    String protocol = args[2];
    String group = args[3];
    String custom_hbase_site_file="_EMPTY_";
    if (args.length == 5) {
      custom_hbase_site_file = args[4];
      LOG.info("== Using custom hbase conf: {}", custom_hbase_site_file);
    }
    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    kafkaParams.put("security.protocol", protocol);
    kafkaParams.put("sasl.kerberos.service.name", "kafka");
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Copy the keytab to our executors
    ssc.sparkContext().addFile(sparkConf.get("spark.yarn.keytab"));

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

    JavaDStream<String> lines = messages.map(ConsumerRecord::value);
    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
            .reduceByKey((i1, i2) -> i1 + i2);
    final StoreCountsToHBase store = new StoreCountsToHBase(sparkConf, custom_hbase_site_file);

    wordCounts.print();

    wordCounts.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
      @Override
      public void call(JavaPairRDD<String, Integer> rdd, Time time) throws IOException {
        store.setTime(time);
        rdd.foreachPartition(store);
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
