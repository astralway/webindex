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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import io.fluo.webindex.core.DataConfig;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Copy {

  private static final Logger log = LoggerFactory.getLogger(Copy.class);

  public static String addSlash(String prefix) {
    if (!prefix.endsWith("/")) {
      return prefix + "/";
    }
    return prefix;
  }

  public static String getFilename(String fullPath) {
    int slashIndex = fullPath.lastIndexOf("/");
    if (slashIndex == -1) {
      return fullPath;
    }
    return fullPath.substring(slashIndex + 1);
  }

  public static FileSystem getHDFS(String hadoopConfDir) throws IOException {
    Configuration config = new Configuration();
    config.addResource(hadoopConfDir);
    return FileSystem.get(config);
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Copy <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    SparkConf sparkConf = new SparkConf().setAppName("CC-Copy");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    Path dataDirPath = new Path(dataConfig.hdfsDataDir);

    FileSystem hdfs = FileSystem.get(ctx.hadoopConfiguration());
    if (!hdfs.exists(dataDirPath)) {
      hdfs.mkdirs(dataDirPath);
    }

    log.info("Copying {} files from AWS to HDFS using {} executors", dataConfig.numFilesToCopy,
        dataConfig.sparkExecutorInstances);

    final String dataDir = addSlash(dataConfig.hdfsDataDir);
    final String urlPrefix = addSlash(dataConfig.ccServerUrl);
    final String hadoopConfDir = dataConfig.hadoopConfDir;

    String fileSetUrl = urlPrefix + dataConfig.ccDataPaths;
    Path fileSetPath = new Path(dataDir + "paths.gz");
    if (!hdfs.exists(fileSetPath)) {
      try (OutputStream out = hdfs.create(fileSetPath);
          BufferedInputStream in = new BufferedInputStream(new URL(fileSetUrl).openStream())) {
        IOUtils.copy(in, out);
      }
      log.info("Copied URL {} to HDFS {}", fileSetUrl, fileSetPath);
    }
    JavaRDD<String> allFiles = ctx.textFile(fileSetPath.toString());

    JavaRDD<String> filesToCopy =
        ctx.parallelize(allFiles.takeOrdered(dataConfig.numFilesToCopy)).repartition(
            dataConfig.numFilesToCopy);

    filesToCopy.foreach(ccPath -> {
      String fn = getFilename(ccPath);
      FileSystem fs = getHDFS(hadoopConfDir);
      Path dfsPath = new Path(dataDir + "data/" + fn);
      if (fs.exists(dfsPath)) {
        log.error("File {} exists in HDFS and should have been previously filtered",
            dfsPath.getName());
        return;
      }

      String urlToCopy = urlPrefix + ccPath;
      log.info("Starting copy of {} to HDFS", urlToCopy);

      // Set replication factor to 1 as we can always recover file from AWS
        try (OutputStream out = fs.create(dfsPath, (short) 1);
            BufferedInputStream in = new BufferedInputStream(new URL(urlToCopy).openStream())) {
          IOUtils.copy(in, out);
        }
        log.info("Created {}", dfsPath.getName());
      });
  }
}
