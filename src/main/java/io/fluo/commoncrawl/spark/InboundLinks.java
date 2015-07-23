package io.fluo.commoncrawl.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Column;
import io.fluo.commoncrawl.inbound.Link;
import io.fluo.commoncrawl.inbound.Page;
import io.fluo.commoncrawl.warc.WARCFileInputFormat;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class InboundLinks {

  private static final Logger log = LoggerFactory.getLogger(InboundLinks.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 4) {
      log.error("Usage: InboundLinks <inputPath> <outputType> <outputLocation> <configPath>");
      System.exit(1);
    }
    log.info("inputPath: {} outputType: {} outputLocation: {} configPath: {}", args[0], args[1],
             args[2], args[3]);
    String outputPath = args[2];

    if (!args[1].equals("hdfs") && !args[1].equals("fluo")) {
      log.error("Unknown output type: "+args[1]);
      System.exit(1);
    }

    if (args[1].equals("fluo") && ((args[3] == null) || !(new File(args[3]).exists()))) {
      log.error("configPath must be set and exist");
      System.exit(-1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("CCInboundLinks");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    Accumulator<Integer> numPages = ctx.accumulator(0);
    Accumulator<Integer> numEmpty = ctx.accumulator(0);
    Accumulator<Integer> numExternalLinks = ctx.accumulator(0);

    final JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(args[0], WARCFileInputFormat.class, Text.class, ArchiveReader.class,
                             new Configuration());

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
              retval.add("u:" + link.getUri() + "\tcount");
              retval.add("u:" + link.getUri() + "\tl:" + page.getLink().getUri() + "\t" + link
                  .getAnchorText());
              retval.add("d:" + link.getReverseTopPrivate() + "\tu:" + link.getUri());
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

    switch (args[1]) {
      case "hdfs":
        sortedCounts.saveAsHadoopFile(outputPath, Text.class, LongWritable.class, TextOutputFormat.class);
        break;
      case "fluo":
        JavaPairRDD<Key, Value> fluoData = sortedCounts.flatMapToPair(
            new PairFlatMapFunction<Tuple2<String, Long>, Key, Value>() {
              @Override
              public Iterable<Tuple2<Key, Value>> call(Tuple2<String, Long> tuple)
                  throws Exception {
                List<Tuple2<Key, Value>> output = new LinkedList<>();
                String[] tupleArgs = tuple._1().split("\t", 2);
                if (tupleArgs.length != 2) {
                  System.out.println("Data lacks tab: " + tuple._1());
                } else {
                  FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
                  fkvg.setRow(tupleArgs[0]).setColumn(new Column(tupleArgs[1]))
                      .setValue(tuple._2().toString());
                  for (FluoKeyValue kv : fkvg.getKeyValues()) {
                    output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
                  }
                }
                return output;
              }
            });

        Job job = Job.getInstance(ctx.hadoopConfiguration());
        AccumuloFileOutputFormat.setOutputPath(job, new Path(outputPath));
        // must use new API here as saveAsHadoopFile throws exception
        fluoData.saveAsNewAPIHadoopFile(outputPath, Key.class, Value.class,
                                            AccumuloFileOutputFormat.class, job.getConfiguration());

        FluoConfiguration config = new FluoConfiguration(new File(args[3]));
        Connector conn = AccumuloUtil.getConnector(config);
        conn.tableOperations().importDirectory(config.getAccumuloTable(), outputPath, "/cc/failures", false);
        log.info("Imported data at {} into Fluo app {}", outputPath, config.getApplicationName());
        break;
      default:
        log.error("Unknown output type: " + args[1]);
    }

    log.info("Num empty = {}", numEmpty.value());
    log.info("Num pages = {}", numPages.value());
    log.info("Num external links = {}", numExternalLinks.value());

    ctx.stop();
  }
}
