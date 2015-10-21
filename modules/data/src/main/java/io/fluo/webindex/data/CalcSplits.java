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
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
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

public class CalcSplits {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("CC-CalcSplits");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    DataConfig dataConfig = DataConfig.load(args[0]);
    IndexStats stats = new IndexStats(ctx);

    final JavaPairRDD<Text, ArchiveReader> archives =
        ctx.newAPIHadoopFile(dataConfig.hdfsDataDir + "/data", WARCFileInputFormat.class,
            Text.class, ArchiveReader.class, new Configuration());

    JavaRDD<Page> pages = IndexUtil.createPages(archives);
    JavaPairRDD<RowColumn, Bytes> accumuloIndex = IndexUtil.createAccumuloIndex(stats, pages);
    SortedSet<Text> splits = IndexUtil.calculateSplits(accumuloIndex, 100);
    splits.forEach(System.out::println);
  }
}
