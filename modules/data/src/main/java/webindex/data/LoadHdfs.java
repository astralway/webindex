/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.WebIndexConfig;
import webindex.core.models.Page;
import webindex.data.fluo.PageLoader;
import webindex.data.spark.IndexEnv;
import webindex.data.util.ArchiveUtil;

public class LoadHdfs {

  private static final Logger log = LoggerFactory.getLogger(LoadHdfs.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: LoadHdfs <dataDir>");
      System.exit(1);
    }
    final String dataDir = args[0];
    IndexEnv.validateDataDir(dataDir);

    final String hadoopConfDir = IndexEnv.getHadoopConfDir();
    final WebIndexConfig webIndexConfig = WebIndexConfig.load();
    final int rateLimit = webIndexConfig.getLoadRateLimit();
    final String appName = webIndexConfig.fluoApp;

    List<String> loadPaths = new ArrayList<>();
    FileSystem hdfs = IndexEnv.getHDFS();
    RemoteIterator<LocatedFileStatus> listIter = hdfs.listFiles(new Path(dataDir), true);
    while (listIter.hasNext()) {
      LocatedFileStatus status = listIter.next();
      if (status.isFile()) {
        loadPaths.add(status.getPath().toString());
      }
    }

    log.info("Loading {} files into Fluo from {}", loadPaths.size(), dataDir);

    SparkConf sparkConf = new SparkConf().setAppName("webindex-load-hdfs");
    try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

      JavaRDD<String> paths = ctx.parallelize(loadPaths, loadPaths.size());

      paths.foreachPartition(iter -> {
        final FluoConfiguration fluoConfig =
            new FluoConfiguration(new File("fluo-conn.properties"));
        fluoConfig.setApplicationName(appName);
        final RateLimiter rateLimiter = rateLimit > 0 ? RateLimiter.create(rateLimit) : null;
        FileSystem fs = IndexEnv.getHDFS(hadoopConfDir);
        try (FluoClient client = FluoFactory.newClient(fluoConfig);
            LoaderExecutor le = client.newLoaderExecutor()) {
          iter.forEachRemaining(path -> {
            Path filePath = new Path(path);
            try {
              if (fs.exists(filePath)) {
                FSDataInputStream fsin = fs.open(filePath);
                ArchiveReader reader = WARCReaderFactory.get(filePath.getName(), fsin, true);
                for (ArchiveRecord record : reader) {
                  Page page = ArchiveUtil.buildPageIgnoreErrors(record);
                  if (page.getOutboundLinks().size() > 0) {
                    log.info("Loading page {} with {} links", page.getUrl(), page
                        .getOutboundLinks().size());
                    if (rateLimiter != null) {
                      rateLimiter.acquire();
                    }
                    le.execute(PageLoader.updatePage(page));
                  }
                }
              }
            } catch (IOException e) {
              log.error("Exception while processing {}", path, e);
            }
          });
        }
      });
    }
  }
}
