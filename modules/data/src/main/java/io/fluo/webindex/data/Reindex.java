/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.webindex.data;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.mapreduce.FluoEntryInputFormat;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reindex {

  private static final Logger log = LoggerFactory.getLogger(Reindex.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Reindex <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    SparkConf sparkConf = new SparkConf().setAppName("CC-Reindex");
    IndexEnv env = new IndexEnv(dataConfig, sparkConf);
    env.makeHdfsTempDirs();

    Job job = Job.getInstance(env.getSparkCtx().hadoopConfiguration());
    FluoEntryInputFormat.configure(job, env.getFluoConfig());

    JavaPairRDD<RowColumn, Bytes> fluoIndex =
        env.getSparkCtx().newAPIHadoopRDD(job.getConfiguration(), FluoEntryInputFormat.class,
            RowColumn.class, Bytes.class);

    JavaPairRDD<RowColumn, Bytes> accumuloIndex = IndexUtil.createAccumuloIndex(fluoIndex);

    // Initialize Accumulo index table with default splits or splits calculated from data
    if (dataConfig.calculateAccumuloSplits) {
      env.initAccumuloIndexTable(IndexUtil.calculateSplits(accumuloIndex, 100));
    } else {
      env.initAccumuloIndexTable(IndexEnv.getDefaultSplits());
    }

    env.saveRowColBytesToAccumulo(accumuloIndex);
  }
}
