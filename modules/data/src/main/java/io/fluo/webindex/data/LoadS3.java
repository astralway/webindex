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
import java.net.URL;
import java.util.List;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.fluo.PageLoader;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.util.ArchiveUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadS3 {

  private static final Logger log = LoggerFactory.getLogger(LoadS3.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: LoadS3 <pathsFile> <range>");
      System.exit(1);
    }
    final List<String> loadList = IndexEnv.getPathsRange(args[0], args[1]);
    if (loadList.isEmpty()) {
      log.error("No files to load given {} {}", args[0], args[1]);
      System.exit(1);
    }

    DataConfig.load();

    SparkConf sparkConf = new SparkConf().setAppName("webindex-load-s3");
    try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

      log.info("Loading {} files (Range {} of paths file {}) from AWS", loadList.size(), args[1],
          args[0]);

      JavaRDD<String> loadRDD = ctx.parallelize(loadList, loadList.size());

      final String prefix = DataConfig.CC_URL_PREFIX;

      loadRDD.foreachPartition(iter -> {
        final FluoConfiguration fluoConfig = new FluoConfiguration(new File("fluo.properties"));
        try (FluoClient client = FluoFactory.newClient(fluoConfig);
            LoaderExecutor le = client.newLoaderExecutor()) {
          iter.forEachRemaining(path -> {
            String urlToCopy = prefix + path;
            log.info("Loading {} to Fluo", urlToCopy);
            try {
              ArchiveReader reader = WARCReaderFactory.get(new URL(urlToCopy), 0);
              for (ArchiveRecord record : reader) {
                Page page = ArchiveUtil.buildPageIgnoreErrors(record);
                if (page.getOutboundLinks().size() > 0) {
                  log.info("Loading page {} with {} links", page.getUrl(), page.getOutboundLinks()
                      .size());
                  le.execute(PageLoader.updatePage(page));
                }
              }
            } catch (Exception e) {
              log.error("Exception while processing {}", path, e);
            }
          });
        }
      });
    }
  }
}
