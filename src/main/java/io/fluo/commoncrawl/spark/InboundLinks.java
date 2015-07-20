package io.fluo.commoncrawl.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.fluo.commoncrawl.inbound.Link;
import io.fluo.commoncrawl.inbound.Page;
import io.fluo.commoncrawl.warc.WARCFileInputFormat;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;
import scala.Tuple2;

public class InboundLinks {

  private static final Logger log = LoggerFactory.getLogger(InboundLinks.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: InboundLinks <inputPath> <outputPath>");
      System.exit(1);
    }
    log.info("Input path: {} Output path: {}", args[0], args[1]);

    SparkConf sparkConf = new SparkConf().setAppName("CCInboundLinks");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

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
              return new ArrayList<>();
            }
            Set<Link> links = page.getExternalLinks();

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

    JavaPairRDD<String, Integer> ones = urls.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();

    sortedCounts.saveAsHadoopFile(args[1], Text.class, LongWritable.class, TextOutputFormat.class);

    ctx.stop();
  }
}
