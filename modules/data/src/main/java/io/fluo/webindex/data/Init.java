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

import java.util.LinkedList;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.mapreduce.FluoKeyValue;
import io.fluo.mapreduce.FluoKeyValueGenerator;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexStats;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.WARCFileInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Init {

  private static final Logger log = LoggerFactory.getLogger(Init.class);
  private static IndexEnv env;

  public static void loadAccumulo(JavaPairRDD<RowColumn, Bytes> linkIndex) throws Exception {
    JavaPairRDD<Key, Value> accumuloData =
        linkIndex.mapToPair(new PairFunction<Tuple2<RowColumn, Bytes>, Key, Value>() {
          @Override
          public Tuple2<Key, Value> call(Tuple2<RowColumn, Bytes> tuple) throws Exception {
            RowColumn rc = tuple._1();
            String row = rc.getRow().toString();
            String cf = rc.getColumn().getFamily().toString();
            String cq = rc.getColumn().getQualifier().toString();
            byte[] val = tuple._2().toArray();
            return new Tuple2<>(new Key(new Text(row), new Text(cf), new Text(cq)), new Value(val));
          }
        });
    env.saveKeyValueToAccumulo(accumuloData);
  }

  public static void loadHDFS(JavaPairRDD<RowColumn, Long> sortedCounts) throws Exception {

    JavaPairRDD<String, Long> stringCounts =
        sortedCounts.mapToPair(t -> new Tuple2<String, Long>(t._1().toString(), t._2()));

    Path hadoopTempDir = env.getHadoopTempDir();
    if (env.getHdfs().exists(hadoopTempDir)) {
      env.getHdfs().delete(hadoopTempDir, true);
    }
    stringCounts.saveAsHadoopFile(hadoopTempDir.toString(), Text.class, LongWritable.class,
        TextOutputFormat.class);
  }

  public static void loadFluo(JavaPairRDD<RowColumn, Bytes> linkIndex) throws Exception {
    JavaPairRDD<Key, Value> fluoData =
        linkIndex.flatMapToPair(new PairFlatMapFunction<Tuple2<RowColumn, Bytes>, Key, Value>() {
          @Override
          public Iterable<Tuple2<Key, Value>> call(Tuple2<RowColumn, Bytes> tuple) throws Exception {
            List<Tuple2<Key, Value>> output = new LinkedList<>();
            RowColumn rc = tuple._1();
            String row = rc.getRow().toString();
            String cf = rc.getColumn().getFamily().toString();
            String cq = rc.getColumn().getQualifier().toString();
            byte[] val = tuple._2().toArray();
            FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
            fkvg.setRow(row).setColumn(new Column(cf, cq)).setValue(val);
            for (FluoKeyValue kv : fkvg.getKeyValues()) {
              output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
            }
            return output;
          }
        });
    env.saveToFluo(fluoData);
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Init <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    try {
      SparkConf sparkConf = new SparkConf().setAppName("CC-Init");
      env = new IndexEnv(dataConfig, sparkConf);
      env.initAccumuloIndexTable();
      env.makeHdfsTempDirs();
    } catch (Exception e) {
      log.error("Env setup failed due to exception", e);
      System.exit(-1);
    }

    IndexStats stats = new IndexStats(env.getSparkCtx());

    final JavaPairRDD<Text, ArchiveReader> archives =
        env.getSparkCtx().newAPIHadoopFile(dataConfig.watDataDir, WARCFileInputFormat.class,
            Text.class, ArchiveReader.class, new Configuration());

    // Create pages RDD from archives
    JavaRDD<Page> pages = IndexUtil.createPages(archives);

    // Create the Accumulo index from pages RDD
    JavaPairRDD<RowColumn, Bytes> accumuloIndex = IndexUtil.createAccumuloIndex(stats, pages);

    // Create a Fluo index by filtering a subset of data from Accumulo index
    JavaPairRDD<RowColumn, Bytes> fluoIndex = IndexUtil.createFluoIndex(accumuloIndex);

    // Load the indexes into Fluo and Accumulo
    loadFluo(fluoIndex);
    loadAccumulo(accumuloIndex);

    stats.print();

    env.getSparkCtx().stop();
  }
}
