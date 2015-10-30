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

import java.io.File;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.fluo.PageLoader;
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

public class Load {

  private static final Logger log = LoggerFactory.getLogger(Load.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Copy <dataConfigPath>");
      System.exit(1);
    }
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir == null) {
      log.error("HADOOP_CONF_DIR must be set in environment!");
      System.exit(1);
    }
    if (!(new File(hadoopConfDir).exists())) {
      log.error("Directory set by HADOOP_CONF_DIR={} does not exist", hadoopConfDir);
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    SparkConf sparkConf = new SparkConf().setAppName("CC-Load");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(dataConfig.getHdfsLoadDir(), WARCFileInputFormat.class, Text.class,
            ArchiveReader.class, new Configuration());

    JavaRDD<Page> pages = IndexUtil.createPages(archives);

    pages.foreachPartition(iter -> {
      final FluoConfiguration fluoConfig = new FluoConfiguration(new File("fluo.properties"));
      try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
        try (LoaderExecutor le = client.newLoaderExecutor()) {
          iter.forEachRemaining(page -> {
            if (page.getOutboundLinks().size() > 0) {
              log.info("Loading page {} with {} links", page.getUrl(), page.getOutboundLinks()
                  .size());
              le.execute(PageLoader.updatePage(page));
            }
          });
        }
      }
    });

    ctx.stop();
  }
}
