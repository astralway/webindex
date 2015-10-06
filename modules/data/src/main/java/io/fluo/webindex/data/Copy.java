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
import java.io.OutputStream;
import java.net.URL;

import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.data.spark.IndexEnv;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Copy {

  private static final Logger log = LoggerFactory.getLogger(Copy.class);

  private static IndexEnv env;

  public static String addSlash(String prefix) {
    if (!prefix.endsWith("/")) {
      return prefix + "/";
    }
    return prefix;
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: Copy <dataConfigPath> <ccPathsFile>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);
    String ccPathsFile = args[1];

    try {
      SparkConf sparkConf = new SparkConf().setAppName("CC-Load");
      env = new IndexEnv(dataConfig, sparkConf);
    } catch (Exception e) {
      log.error("Env setup failed due to exception", e);
      System.exit(-1);
    }

    Path dataDirPath = new Path(dataConfig.hdfsDataDir);
    if (!env.getHdfs().exists(dataDirPath)) {
      env.getHdfs().mkdirs(dataDirPath);
    }

    log.info("Copying {} files from AWS to HDFS using {} executors", dataConfig.numFilesToCopy,
        dataConfig.sparkExecutorInstances);

    JavaRDD<String> allFiles = env.getSparkCtx().textFile(ccPathsFile);

    JavaRDD<String> filesToLoad =
        env.getSparkCtx().parallelize(allFiles.takeOrdered(dataConfig.numFilesToCopy));

    final String urlPrefix = addSlash(dataConfig.ccServerUrl);
    final String dataDir = addSlash(dataConfig.hdfsDataDir);
    final String hadoopConfDir = dataConfig.hadoopConfDir;

    filesToLoad.foreach(ccPath -> {

      int slashIndex = ccPath.lastIndexOf("/");
      if (slashIndex == -1) {
        log.error("CC Path does not contain slash: {}", ccPath);
        return;
      }
      String fn = ccPath.substring(slashIndex + 1);

      Configuration config = new Configuration();
      config.addResource(hadoopConfDir);
      Path dfsPath = new Path(dataDir + fn);
      FileSystem hdfs = FileSystem.get(config);
      if (hdfs.exists(dfsPath)) {
        log.info("File {} already exists in HDFS", dfsPath.getName());
        return;
      }

      String urlToCopy = urlPrefix + ccPath;
      log.info("Starting copy of {} to HDFS", urlToCopy);

      try (OutputStream out = hdfs.create(dfsPath);
          BufferedInputStream in = new BufferedInputStream(new URL(urlToCopy).openStream())) {
        IOUtils.copy(in, out);
      }
      log.info("Created {}", dfsPath.getName());
    });
  }
}
