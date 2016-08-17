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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.WebIndexConfig;
import webindex.data.spark.IndexEnv;

public class Copy {

  private static final Logger log = LoggerFactory.getLogger(Copy.class);

  public static String getFilename(String fullPath) {
    int slashIndex = fullPath.lastIndexOf("/");
    if (slashIndex == -1) {
      return fullPath;
    }
    return fullPath.substring(slashIndex + 1);
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      log.error("Usage: Copy <pathsFile> <range> <dest>");
      System.exit(1);
    }
    final String hadoopConfDir = IndexEnv.getHadoopConfDir();
    final List<String> copyList = IndexEnv.getPathsRange(args[0], args[1]);
    if (copyList.isEmpty()) {
      log.error("No files to copy given {} {}", args[0], args[1]);
      System.exit(1);
    }

    WebIndexConfig webIndexConfig = WebIndexConfig.load();

    SparkConf sparkConf = new SparkConf().setAppName("webindex-copy");
    try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

      FileSystem hdfs = FileSystem.get(ctx.hadoopConfiguration());
      Path destPath = new Path(args[2]);
      if (!hdfs.exists(destPath)) {
        hdfs.mkdirs(destPath);
      }

      log.info("Copying {} files (Range {} of paths file {}) from AWS to HDFS {}", copyList.size(),
          args[1], args[0], destPath.toString());

      JavaRDD<String> copyRDD = ctx.parallelize(copyList, webIndexConfig.getNumExecutorInstances());

      final String prefix = WebIndexConfig.CC_URL_PREFIX;
      final String destDir = destPath.toString();

      copyRDD
          .foreachPartition(iter -> {
            FileSystem fs = IndexEnv.getHDFS(hadoopConfDir);
            iter.forEachRemaining(ccPath -> {
              try {
                Path dfsPath = new Path(destDir + "/" + getFilename(ccPath));
                if (fs.exists(dfsPath)) {
                  log.error("File {} exists in HDFS and should have been previously filtered",
                      dfsPath.getName());
                } else {
                  String urlToCopy = prefix + ccPath;
                  log.info("Starting copy of {} to {}", urlToCopy, destDir);
                  try (OutputStream out = fs.create(dfsPath);
                      BufferedInputStream in =
                          new BufferedInputStream(new URL(urlToCopy).openStream())) {
                    IOUtils.copy(in, out);
                  }
                  log.info("Created {}", dfsPath.getName());
                }
              } catch (IOException e) {
                log.error("Exception while copying {}", ccPath, e);
              }
            });
          });
    }
  }
}
