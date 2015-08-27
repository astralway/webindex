package io.fluo.commoncrawl.data;

import java.util.LinkedList;
import java.util.List;

import io.fluo.api.data.Column;
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.data.spark.IndexEnv;
import io.fluo.commoncrawl.data.spark.IndexStats;
import io.fluo.commoncrawl.data.spark.IndexUtil;
import io.fluo.commoncrawl.data.util.WARCFileInputFormat;
import io.fluo.mapreduce.FluoKeyValue;
import io.fluo.mapreduce.FluoKeyValueGenerator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Init {

  private static final Logger log = LoggerFactory.getLogger(Init.class);
  private static IndexEnv env;

  public static void loadAccumulo(JavaPairRDD<String, Long> sortedCounts) throws Exception {

    JavaPairRDD<String, Long> filteredSortedCounts = sortedCounts.filter(
        new Function<Tuple2<String, Long>, Boolean>() {
          @Override
          public Boolean call(Tuple2<String, Long> t1) throws Exception {
            String[] keyArgs = t1._1().split("\t", 2);
            String row = keyArgs[0];
            String cf = keyArgs[1].split(":", 2)[0];
            return !(row.startsWith("d:") && cf.equals(ColumnConstants.PAGES));
          }
        });

    JavaPairRDD<Key, Value> accumuloData = filteredSortedCounts.mapToPair(
        new PairFunction<Tuple2<String, Long>, Key, Value>() {
          @Override
          public Tuple2<Key, Value> call(Tuple2<String, Long> tuple)
              throws Exception {
            String[] keyArgs = tuple._1().split("\t", 2);
            if (keyArgs.length != 2) {
              return null;
            }
            String row = keyArgs[0];
            String cf = keyArgs[1].split(":", 2)[0];
            String cq = keyArgs[1].split(":", 2)[1];
            byte[] val = tuple._2().toString().getBytes();
            if (cf.equals(ColumnConstants.INLINKS) ||
                (cf.equals(ColumnConstants.PAGE) && cq.startsWith(ColumnConstants.CUR))) {
              String[] tempArgs = cq.split("\t", 2);
              cq = tempArgs[0];
              if (tuple._2() > 1) {
                log.info("Found key {} with count of {}", tuple._1(), tuple._2().toString());
              }
              val = tempArgs[1].getBytes();
            }
            return new Tuple2<>(new Key(new Text(row), new Text(cf), new Text(cq)), new Value(val));
          }
        });
    env.saveKeyValueToAccumulo(accumuloData);
  }

  public static void loadHDFS(JavaPairRDD<String, Long> sortedCounts) throws Exception {
    Path hadoopTempDir = env.getHadoopTempDir();
    if (env.getHdfs().exists(hadoopTempDir)) {
      env.getHdfs().delete(hadoopTempDir, true);
    }
    sortedCounts.saveAsHadoopFile(hadoopTempDir.toString(), Text.class, LongWritable.class,
                                  TextOutputFormat.class);
  }

  public static void loadFluo(JavaPairRDD<String, Long> sortedCounts) throws Exception {
    JavaPairRDD<Key, Value> fluoData = sortedCounts.flatMapToPair(
        new PairFlatMapFunction<Tuple2<String, Long>, Key, Value>() {
          @Override
          public Iterable<Tuple2<Key, Value>> call(Tuple2<String, Long> tuple)
              throws Exception {
            List<Tuple2<Key, Value>> output = new LinkedList<>();
            String[] keyArgs = tuple._1().split("\t", 2);
            if (keyArgs.length != 2) {
              System.out.println("Data lacks tab: " + tuple._1());
              return output;
            }
            String row = keyArgs[0];
            String cf = keyArgs[1].split(":", 2)[0];
            String cq = keyArgs[1].split(":", 2)[1];
            byte[] val = tuple._2().toString().getBytes();

            if (cf.equals(ColumnConstants.RANK) || cq.equals(ColumnConstants.RANK) ||
                (row.startsWith("d:") && cf.equals(ColumnConstants.PAGES))) {
              return output;
            }
            if (cf.equals(ColumnConstants.INLINKS) ||
                (cf.equals(ColumnConstants.PAGE) && cq.startsWith(ColumnConstants.CUR))) {
              String[] tempArgs = cq.split("\t", 2);
              cq = tempArgs[0];
              val = tempArgs[1].getBytes();
            }
            FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
            fkvg.setRow(row).setColumn(new Column(cf, cq)).setValue(val);
            for (FluoKeyValue kv : fkvg.getKeyValues()) {
              output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
            }
            return output;
          }
        });
    env.saveToFluo(fluoData);
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Init <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    try {
      SparkConf sparkConf = new SparkConf().setAppName("CC-Init");
      env = new IndexEnv(dataConfig, sparkConf);
      env.initAccumuloIndexTable();
      env.makeHdfsTempDirs();
    } catch (Exception e) {
      log.error("Env setup failed due to exception", e);
      System.exit(-1);
    }

    IndexStats stats = new IndexStats(env.getSparkCtx());

    final JavaPairRDD<Text, ArchiveReader> archives =
        env.getSparkCtx().newAPIHadoopFile(dataConfig.watDataDir, WARCFileInputFormat.class,
                                           Text.class, ArchiveReader.class, new Configuration());

    JavaPairRDD<String, Long> sortedLinkCounts = IndexUtil.createLinkCounts(stats, archives);

    JavaPairRDD<String, Long> sortedTopCounts = IndexUtil.createSortedTopCounts(sortedLinkCounts);

    // Load intermediate results into Fluo
    loadFluo(sortedTopCounts);

    // Load final indexes into Accumulo
    loadAccumulo(sortedTopCounts);

    // For testing, Load into HDFS
    //loadHDFS(sortedTopCounts);

    stats.print();

    env.getSparkCtx().stop();
  }
}
