package io.fluo.commoncrawl.wordcount;

import io.fluo.commoncrawl.warc.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: " + this.getClass().getSimpleName() + " <inputPath> <outputPath>");
      System.exit(1);
    }
    String inputPath = args[0];
    String outputPath = args[1];
    log.info("Input path: " + inputPath);
    log.info("Output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(WordCount.class);
    job.setNumReduceTasks(1);

    job.setInputFormatClass(WARCFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));

    FileSystem fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(WordCountMap.WordCountMapper.class);
    job.setReducerClass(LongSumReducer.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
