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

import java.net.URL;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.WebIndexConfig;
import webindex.data.spark.IndexEnv;
import webindex.data.util.ArchiveUtil;

public class TestParser {

  private static final Logger log = LoggerFactory.getLogger(TestParser.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: TestParser <pathsFile> <range>");
      System.exit(1);
    }
    final List<String> loadList = IndexEnv.getPathsRange(args[0], args[1]);
    if (loadList.isEmpty()) {
      log.error("No files to load given {} {}", args[0], args[1]);
      System.exit(1);
    }

    WebIndexConfig.load();

    SparkConf sparkConf = new SparkConf().setAppName("webindex-test-parser");
    try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

      log.info("Parsing {} files (Range {} of paths file {}) from AWS", loadList.size(), args[1],
          args[0]);

      JavaRDD<String> loadRDD = ctx.parallelize(loadList, loadList.size());

      final String prefix = WebIndexConfig.CC_URL_PREFIX;

      loadRDD.foreachPartition(iter -> iter.forEachRemaining(path -> {
        String urlToCopy = prefix + path;
        log.info("Parsing {}", urlToCopy);
        try {
          ArchiveReader reader = WARCReaderFactory.get(new URL(urlToCopy), 0);
          for (ArchiveRecord record : reader) {
            ArchiveUtil.buildPageIgnoreErrors(record);
          }
        } catch (Exception e) {
          log.error("Exception while processing {}", path, e);
        }
      }));
    }
  }
}
