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

import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexStats;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Init {

  private static final Logger log = LoggerFactory.getLogger(Init.class);

  public static void main(String[] args) throws Exception {

    if (args.length > 1) {
      log.error("Usage: Init [<dataDir>]");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load();

    IndexEnv env = new IndexEnv(dataConfig);
    env.setFluoTableSplits();
    log.info("Initialized Fluo table splits");

    if (args.length == 1) {
      final String dataDir = args[0];
      IndexEnv.validateDataDir(dataDir);

      SparkConf sparkConf = new SparkConf().setAppName("webindex-init");
      JavaSparkContext ctx = new JavaSparkContext(sparkConf);
      IndexStats stats = new IndexStats(ctx);

      final JavaPairRDD<Text, ArchiveReader> archives =
          ctx.newAPIHadoopFile(dataDir, WARCFileInputFormat.class, Text.class, ArchiveReader.class,
              new Configuration());

      JavaRDD<Page> pages = IndexUtil.createPages(archives);

      env.initializeIndexes(ctx, pages, stats);

      stats.print();
      ctx.stop();
    } else {
      log.info("An init data dir was not specified");
    }
  }
}
