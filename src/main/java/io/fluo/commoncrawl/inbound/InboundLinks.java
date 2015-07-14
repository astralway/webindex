/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.commoncrawl.inbound;

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

public class InboundLinks extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(InboundLinks.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InboundLinks(), args);
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
    job.setJarByClass(InboundLinks.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileSystem fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(WARCFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(LinkMap.ServerMapper.class);
    job.setReducerClass(LongSumReducer.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
