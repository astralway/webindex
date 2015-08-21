package io.fluo.commoncrawl.data;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.data.spark.IndexStats;
import io.fluo.commoncrawl.data.spark.IndexUtil;
import io.fluo.commoncrawl.data.util.WARCFileInputFormat;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.mapreduce.FluoKeyValue;
import io.fluo.mapreduce.FluoKeyValueGenerator;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Init {

  private static final Logger log = LoggerFactory.getLogger(Init.class);
  private static DataConfig dataConfig;
  private static Connector conn;
  private static JavaSparkContext ctx;
  private static FluoConfiguration fluoConfig;
  private static FileSystem hdfs;
  private static Path failuresDir;

  public static void loadAccumulo(JavaPairRDD<String, Long> sortedCounts) throws Exception {
    JavaPairRDD<Key, Value> accumuloData = sortedCounts.mapToPair(
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
    Job accJob = Job.getInstance(ctx.hadoopConfiguration());

    Path accumuloTempDir = new Path(dataConfig.hdfsTempDir + "/accumulo");
    if (hdfs.exists(accumuloTempDir)) {
      hdfs.delete(accumuloTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(accJob, accumuloTempDir);
    // must use new API here as saveAsHadoopFile throws exception
    accumuloData.saveAsNewAPIHadoopFile(accumuloTempDir.toString(), Key.class, Value.class,
                                        AccumuloFileOutputFormat.class, accJob.getConfiguration());
    conn.tableOperations().importDirectory(dataConfig.accumuloIndexTable,
                                           accumuloTempDir.toString(), failuresDir.toString(),
                                           false);
  }

  public static void loadHDFS(JavaPairRDD<String, Long> sortedCounts) throws Exception {
    Path hadoopTempDir = new Path(dataConfig.hdfsTempDir + "/hadoop");
    if (hdfs.exists(hadoopTempDir)) {
      hdfs.delete(hadoopTempDir, true);
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
            } else {
              FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
              fkvg.setRow(keyArgs[0]).setColumn(new Column(keyArgs[1])).setValue(Bytes.of("1"));
              for (FluoKeyValue kv : fkvg.getKeyValues()) {
                output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
              }
            }
            return output;
          }
        });

    Job job = Job.getInstance(ctx.hadoopConfiguration());
    Path fluoTempDir = new Path(dataConfig.hdfsTempDir + "/fluo");
    if (hdfs.exists(fluoTempDir)) {
      hdfs.delete(fluoTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(job, fluoTempDir);
    // must use new API here as saveAsHadoopFile throws exception
    fluoData.saveAsNewAPIHadoopFile(fluoTempDir.toString(), Key.class, Value.class,
                                    AccumuloFileOutputFormat.class, job.getConfiguration());
    conn.tableOperations().importDirectory(fluoConfig.getAccumuloTable(), fluoTempDir.toString(),
                                           failuresDir.toString(), false);
    log.info("Imported data at {} into Fluo app {}", fluoTempDir, fluoConfig.getApplicationName());
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Init <dataConfigPath>");
      System.exit(1);
    }
    dataConfig = DataConfig.load(args[0]);

    if ((dataConfig.fluoPropsPath == null) || !(new File(dataConfig.fluoPropsPath).exists())) {
      log.error("fluoPropsPath must be set in data.yml and exist");
      System.exit(-1);
    }

    fluoConfig = new FluoConfiguration(new File(dataConfig.fluoPropsPath));
    conn = AccumuloUtil.getConnector(fluoConfig);
    if (conn.tableOperations().exists(dataConfig.accumuloIndexTable)) {
      conn.tableOperations().delete(dataConfig.accumuloIndexTable);
    }
    conn.tableOperations().create(dataConfig.accumuloIndexTable);

    SparkConf sparkConf = new SparkConf().setAppName("CC-Init");
    ctx = new JavaSparkContext(sparkConf);
    IndexStats stats = new IndexStats(ctx);

    hdfs = FileSystem.get(ctx.hadoopConfiguration());
    Path tempDir = new Path(dataConfig.hdfsTempDir);
    if (!hdfs.exists(tempDir)) {
      hdfs.mkdirs(tempDir);
    }
    failuresDir = new Path(dataConfig.hdfsTempDir + "/failures");
    if (!hdfs.exists(failuresDir)) {
      hdfs.mkdirs(failuresDir);
    }

    final JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(dataConfig.watDataDir, WARCFileInputFormat.class, Text.class,
                             ArchiveReader.class, new Configuration());

    JavaPairRDD<String, Long> sortedLinkCounts = IndexUtil.createLinkCounts(stats, archives);

    // Load intermediate results into Fluo
    //loadFluo(sortedLinkCounts);

    JavaPairRDD<String, Long> sortedTopCounts = IndexUtil.createSortedTopCounts(sortedLinkCounts);

    // Load final indexes into Accumulo
    loadAccumulo(sortedTopCounts);

    // For testing, Load into HDFS
    //loadHDFS(sortedTopCounts);

    stats.print();

    ctx.stop();
  }
}
