package io.fluo.commoncrawl.data.spark;

import io.fluo.commoncrawl.data.warc.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public final class WordCount {

  private static final Logger log = LoggerFactory.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: WordCount <inputPath> <outputPath>");
      System.exit(1);
    }
    log.info("Input path: {} Output path: {}", args[0], args[1]);

    SparkConf sparkConf = new SparkConf().setAppName("CCWordCount");
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

    JavaRDD<String> words = records.flatMap(r -> new WordTokenizer(r));

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2 );

    counts.saveAsHadoopFile(args[1], Text.class, LongWritable.class, TextOutputFormat.class);

    ctx.stop();
  }
}
