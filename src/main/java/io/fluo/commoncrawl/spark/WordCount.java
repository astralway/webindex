package io.fluo.commoncrawl.spark;

import io.fluo.commoncrawl.warc.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public final class WordCount {

  private static final Logger log = LoggerFactory.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: " + WordCount.class.getSimpleName() + " <inputPath> <outputPath>");
      System.exit(1);
    }
    String inputPath = args[0];
    String outputPath = args[1];
    log.info("Input path: " + inputPath);
    log.info("Output path: " + outputPath);

    SparkConf sparkConf = new SparkConf().setAppName("CCWordCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    log.info("Min partions: " + ctx.defaultMinPartitions());

    final JavaPairRDD<Text, ArchiveReader> archives = ctx.newAPIHadoopFile(inputPath,
        WARCFileInputFormat.class, Text.class, ArchiveReader.class, new Configuration());

    log.info("Count: " + archives.count());

    JavaRDD<String> words = archives.flatMap(new FlatMapFunction<Tuple2<Text,ArchiveReader>,String>() {
      @Override public Iterable<String> call(Tuple2<Text,ArchiveReader> tuple) throws Exception {
        return new WordTokenizer(tuple._2());
      }});

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String,String,Integer>() {
      @Override public Tuple2<String,Integer> call(String s) {
        return new Tuple2<String,Integer>(s, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
        new Function2<Integer,Integer,Integer>() {
          @Override public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

    counts.saveAsHadoopFile(outputPath, Text.class, LongWritable.class, TextOutputFormat.class);

    ctx.stop();
  }
}
