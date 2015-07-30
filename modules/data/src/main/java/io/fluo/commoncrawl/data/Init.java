package io.fluo.commoncrawl.data;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.data.inbound.Link;
import io.fluo.commoncrawl.data.inbound.Page;
import io.fluo.commoncrawl.data.warc.WARCFileInputFormat;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.mapreduce.FluoKeyValue;
import io.fluo.mapreduce.FluoKeyValueGenerator;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
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
            } else {
              return new Tuple2<>(new Key(new Text(keyArgs[0]), new Text(keyArgs[1])),
                                  new Value(tuple._2().toString().getBytes()));
            }
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
                                           accumuloTempDir.toString(), failuresDir.toString(), false);
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
            } else if (keyArgs[0].startsWith("p:") && keyArgs[1].startsWith("p:")) {
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
    if (!conn.tableOperations().exists(dataConfig.accumuloIndexTable)) {
      conn.tableOperations().create(dataConfig.accumuloIndexTable);
    }

    SparkConf sparkConf = new SparkConf().setAppName("CC-Init");
    ctx = new JavaSparkContext(sparkConf);

    hdfs = FileSystem.get(ctx.hadoopConfiguration());
    Path tempDir = new Path(dataConfig.hdfsTempDir);
    if (!hdfs.exists(tempDir)) {
      hdfs.mkdirs(tempDir);
    }
    failuresDir = new Path(dataConfig.hdfsTempDir + "/failures");
    if (!hdfs.exists(failuresDir)) {
      hdfs.mkdirs(failuresDir);
    }

    Accumulator<Integer> numPages = ctx.accumulator(0);
    Accumulator<Integer> numEmpty = ctx.accumulator(0);
    Accumulator<Integer> numExternalLinks = ctx.accumulator(0);

    final JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(dataConfig.watDataDir, WARCFileInputFormat.class, Text.class,
                             ArchiveReader.class, new Configuration());

    JavaRDD<ArchiveRecord> records = archives.flatMap(
        new FlatMapFunction<Tuple2<Text, ArchiveReader>, ArchiveRecord>() {
          @Override
          public Iterable<ArchiveRecord> call(Tuple2<Text, ArchiveReader> tuple) throws Exception {
            return tuple._2();
          }
        });

    JavaRDD<Page> pages = records.map(r -> Page.fromIgnoringErrors(r));

    JavaRDD<String> urls = pages.flatMap(
        new FlatMapFunction<Page, String>() {
          @Override
          public Iterable<String> call(Page page) throws Exception {
            if (page.isEmpty()) {
              numEmpty.add(1);
              return new ArrayList<>();
            }
            numPages.add(1);
            Set<Link> links = page.getExternalLinks();
            numExternalLinks.add(links.size());

            List<String> retval = new ArrayList<>();
            for (Link link : links) {
              retval.add("p:" + link.getUri() + "\tcount");
              retval.add("p:" + link.getUri() + "\tp:" + page.getLink().getUri() + "\t" + link
                  .getAnchorText());
              retval.add("d:" + link.getReverseTopPrivate() + "\tp:" + link.getUri());
            }
            return retval;
          }
        });

    final Long one = (long) 1;
    JavaPairRDD<String, Long> ones = urls.mapToPair(s -> new Tuple2<>(s, one));
    ones.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<String, Long> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Long> topCounts = counts.mapToPair(
        new PairFunction<Tuple2<String, Long>, String, Long>() {
          @Override
          public Tuple2<String, Long> call(Tuple2<String, Long> t)
              throws Exception {
            if (t._1().startsWith("d:")) {
              String[] args = t._1().split("\t", 2);
              String domain = args[0];
              String link = args[1];
              Long numLinks = t._2();
              Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
              String numLinksEnc = Hex.encodeHexString(lexicoder.encode(numLinks));
              return new Tuple2<>(String.format("%s\t%s\t%s", domain, numLinksEnc, link),
                                  numLinks);
            }
            return t;
          }
        });
    topCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<String, Long> sortedCounts = topCounts.sortByKey();

    loadAccumulo(sortedCounts);

    // Loading into HDFS only used for testing
    //loadHDFS(sortedCounts);

    loadFluo(sortedCounts);

    log.info("Num empty = {}", numEmpty.value());
    log.info("Num pages = {}", numPages.value());
    log.info("Num external links = {}", numExternalLinks.value());

    ctx.stop();
  }
}
