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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.mapreduce.FluoEntryInputFormat;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.DataUtil;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.LinkUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Reindex {

  private static final Logger log = LoggerFactory.getLogger(Reindex.class);

  private static IndexEnv env;

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Reindex <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    try {
      SparkConf sparkConf = new SparkConf().setAppName("CC-Reindex");
      env = new IndexEnv(dataConfig, sparkConf);
      env.initAccumuloIndexTable();
      env.makeHdfsTempDirs();
    } catch (Exception e) {
      log.error("Env setup failed due to exception", e);
      System.exit(-1);
    }

    Job job = Job.getInstance(env.getSparkCtx().hadoopConfiguration());
    FluoEntryInputFormat.configure(job, env.getFluoConfig());

    JavaPairRDD<RowColumn, Bytes> fluoData =
        env.getSparkCtx().newAPIHadoopRDD(job.getConfiguration(), FluoEntryInputFormat.class,
            RowColumn.class, Bytes.class);

    JavaPairRDD<RowColumn, Bytes> indexData = IndexUtil.reindexFluo(fluoData);

    env.saveRowColBytesToAccumulo(indexData);
  }
}
