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

import java.util.SortedSet;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
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

public class CalcSplits {

  private static final Logger log = LoggerFactory.getLogger(CalcSplits.class);

  public static void main(String[] args) {
    if (args.length != 1) {
      log.error("Usage: CalcSplits <dataDir>");
      System.exit(1);
    }
    final String dataDir = args[0];
    IndexEnv.validateDataDir(dataDir);

    SparkConf sparkConf = new SparkConf().setAppName("webindex-calcsplits");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    IndexStats stats = new IndexStats(ctx);

    final JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(dataDir, WARCFileInputFormat.class, Text.class, ArchiveReader.class,
            new Configuration());

    JavaRDD<Page> pages = IndexUtil.createPages(archives);

    JavaPairRDD<String, UriInfo> uriMap = IndexUtil.createUriMap(pages);
    JavaPairRDD<String, Long> domainMap = IndexUtil.createDomainMap(uriMap);
    JavaPairRDD<RowColumn, Bytes> accumuloIndex =
        IndexUtil.createAccumuloIndex(stats, pages, uriMap, domainMap);
    SortedSet<Text> splits = IndexUtil.calculateSplits(accumuloIndex, 100);
    log.info("Accumulo splits:");
    splits.forEach(System.out::println);

    ctx.stop();
  }
}
