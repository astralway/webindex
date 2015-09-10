package io.fluo.webindex.data;

import java.util.LinkedList;
import java.util.List;

import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexStats;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.WARCFileInputFormat;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Init {

  private static final Logger log = LoggerFactory.getLogger(Init.class);
  private static IndexEnv env;

  public static void loadAccumulo(JavaPairRDD<RowColumn, Long> sortedCounts) throws Exception {

    JavaPairRDD<RowColumn, Long> filteredSortedCounts =
        sortedCounts.filter(t -> !(t._1().getRow().toString().startsWith("d:") && t._1()
            .getColumn().getFamily().toString().equals(Constants.PAGES)));

    JavaPairRDD<Key, Value> accumuloData =
        filteredSortedCounts.mapToPair(new PairFunction<Tuple2<RowColumn, Long>, Key, Value>() {
          @Override
          public Tuple2<Key, Value> call(Tuple2<RowColumn, Long> tuple) throws Exception {
            RowColumn rc = tuple._1();
            String row = rc.getRow().toString();
            String cf = rc.getColumn().getFamily().toString();
            String cq = rc.getColumn().getQualifier().toString();
            byte[] val = tuple._2().toString().getBytes();
            if (cf.equals(Constants.INLINKS)
                || (cf.equals(Constants.PAGE) && cq.startsWith(Constants.CUR))) {
              if (tuple._2() > 1) {
                log.info("Found key {} with count of {}", tuple._1(), tuple._2().toString());
              }
              val = rc.getColumn().getVisibility().toArray();
            }
            return new Tuple2<>(new Key(new Text(row), new Text(cf), new Text(cq)), new Value(val));
          }
        });
    env.saveKeyValueToAccumulo(accumuloData);
  }

  public static void loadHDFS(JavaPairRDD<RowColumn, Long> sortedCounts) throws Exception {

    JavaPairRDD<String, Long> stringCounts =
        sortedCounts.mapToPair(t -> new Tuple2<String, Long>(t._1().toString(), t._2()));

    Path hadoopTempDir = env.getHadoopTempDir();
    if (env.getHdfs().exists(hadoopTempDir)) {
      env.getHdfs().delete(hadoopTempDir, true);
    }
    stringCounts.saveAsHadoopFile(hadoopTempDir.toString(), Text.class, LongWritable.class,
        TextOutputFormat.class);
  }

  public static void loadFluo(JavaPairRDD<RowColumn, Long> sortedCounts) throws Exception {
    JavaPairRDD<Key, Value> fluoData =
        sortedCounts.flatMapToPair(new PairFlatMapFunction<Tuple2<RowColumn, Long>, Key, Value>() {
          @Override
          public Iterable<Tuple2<Key, Value>> call(Tuple2<RowColumn, Long> tuple) throws Exception {
            List<Tuple2<Key, Value>> output = new LinkedList<>();
            RowColumn rc = tuple._1();
            String row = rc.getRow().toString();
            String cf = rc.getColumn().getFamily().toString();
            String cq = rc.getColumn().getQualifier().toString();
            byte[] val = tuple._2().toString().getBytes();

            if (cf.equals(Constants.RANK) || cq.equals(Constants.RANK)
                || (row.startsWith("d:") && cf.equals(Constants.PAGES))) {
              return output;
            }
            if (cf.equals(Constants.INLINKS)
                || (cf.equals(Constants.PAGE) && cq.startsWith(Constants.CUR))) {
              val = rc.getColumn().getVisibility().toArray();
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

    JavaPairRDD<RowColumn, Long> sortedLinkCounts = IndexUtil.createLinkCounts(stats, archives);

    JavaPairRDD<RowColumn, Long> sortedTopCounts =
        IndexUtil.createSortedTopCounts(sortedLinkCounts);

    // Load intermediate results into Fluo
    loadFluo(sortedTopCounts);

    // Load final indexes into Accumulo
    loadAccumulo(sortedTopCounts);

    // For testing, Load into HDFS
    // loadHDFS(sortedTopCounts);

    stats.print();

    env.getSparkCtx().stop();
  }
}
