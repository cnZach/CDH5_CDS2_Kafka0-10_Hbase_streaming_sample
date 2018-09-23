package org.hbase.myexample.spark;


import scala.Tuple2;
import com.google.common.collect.Lists;

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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.kafka.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import kafka.serializer.StringDecoder;


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
 * Usage: JavaKafka08WordCountStoreInHBase <broker-list> <topic>
 */
public final class JavaKafka08WordCountStoreInHBase {

  private static final Logger LOG = LoggerFactory.getLogger(JavaKafka08WordCountStoreInHBase.class);

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
    private final Tuple2<String, String> auth;

    public StoreCountsToHBase(final SparkConf sparkConf) {
      auth = new Tuple2<String,String>(sparkConf.get("spark.yarn.principal"), sparkConf.get("spark.yarn.keytab"));
    }

    public void setTime(Time time) {
      ROW_ID = Bytes.toBytes(time.toString());
    }

    /**
     * Rely on a map of (principal,keytab) => connection to ensure we only keep around one per
     * Classloader.
     */
    private static Tuple2<UserGroupInformation, Connection> ensureConnection(final Tuple2<String, String> auth)
            throws IOException, InterruptedException {
      Tuple2<UserGroupInformation, Connection> result = connections.get(auth);
      if (result == null) {
        LOG.info("Setting up HBase connection.");
        try {
          final SparkHadoopUtil util = SparkHadoopUtil.get();
          final Configuration conf = HBaseConfiguration.create(util.newConfiguration(new SparkConf()));
          // This work-around for getting hbase client configs requires that you deploy an HBase GATEWAY
          // role on each node that can run a spark executor.
          final File clientConfigs = new File("/etc/hbase/conf");
          for (File siteConfig : clientConfigs.listFiles()) {
            if (siteConfig.getName().endsWith(".xml")) {
              LOG.debug("Adding config resource: {}", siteConfig);
              conf.addResource(siteConfig.toURI().toURL());
            }
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
    if (args.length < 2) {
      System.err.println("Usage: JavaKafka08WordCountStoreInHBase <broker-list> <topic>");
      System.exit(1);
    }

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafka08WordCountStoreInHBase");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    String brokers = args[0];
    String topics = args[1];
    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Copy the keytab to our executors
    ssc.sparkContext().addFile(sparkConf.get("spark.yarn.keytab"));

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
            ssc,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            kafkaParams,
            topicsSet
    );

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });


    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
              }
            }).reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
              }
            });

    final StoreCountsToHBase store = new StoreCountsToHBase(sparkConf);

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
