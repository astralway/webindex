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

package io.fluo.webindex.data.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.core.util.SpanUtil;
import io.fluo.webindex.core.DataConfig;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class IndexEnv {

  private static final Logger log = LoggerFactory.getLogger(IndexEnv.class);

  private JavaSparkContext sparkCtx;
  private DataConfig dataConfig;
  private Connector conn;
  private FluoConfiguration fluoConfig;
  private FileSystem hdfs;
  private Path failuresDir;
  private Path hadoopTempDir;
  private Path accumuloTempDir;
  private Path fluoTempDir;

  public IndexEnv(DataConfig dataConfig, SparkConf sparkConf) throws IOException {
    this.dataConfig = dataConfig;
    Preconditions.checkNotNull(dataConfig.getFluoPropsPath());
    Preconditions.checkArgument(new File(dataConfig.getFluoPropsPath()).exists(),
        "fluoPropsPath must be set in data.yml and exist");
    fluoConfig = new FluoConfiguration(new File(dataConfig.getFluoPropsPath()));
    conn = AccumuloUtil.getConnector(fluoConfig);
    sparkCtx = new JavaSparkContext(sparkConf);
    hdfs = FileSystem.get(sparkCtx.hadoopConfiguration());
    hadoopTempDir = new Path(dataConfig.hdfsTempDir + "/hadoop");
    fluoTempDir = new Path(dataConfig.hdfsTempDir + "/fluo");
    failuresDir = new Path(dataConfig.hdfsTempDir + "/failures");
    accumuloTempDir = new Path(dataConfig.hdfsTempDir + "/accumulo");
  }

  private static SortedSet<Text> getSplits(String filename) {
    SortedSet<Text> splits = new TreeSet<>();
    InputStream is = IndexEnv.class.getClassLoader().getResourceAsStream("splits/" + filename);
    try {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
        String line;
        while ((line = br.readLine()) != null) {
          splits.add(new Text(line));
        }
      }
    } catch (IOException e) {
      log.error("Failed to read splits/accumulo-default.txt resource", e);
      System.exit(-1);
    }
    return splits;
  }

  public static SortedSet<Text> getAccumuloDefaultSplits() {
    return getSplits("accumulo-default.txt");
  }

  public static SortedSet<Text> getFluoDefaultSplits() {
    return getSplits("fluo-default.txt");
  }

  public void initAccumuloIndexTable(SortedSet<Text> splits) {
    final String table = dataConfig.accumuloIndexTable;
    if (conn.tableOperations().exists(table)) {
      try {
        conn.tableOperations().delete(table);
      } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
        throw new IllegalStateException("Failed to delete Accumulo table " + table, e);
      }
    }
    try {
      conn.tableOperations().create(table);
    } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
      throw new IllegalStateException("Failed to create Accumulo table " + table, e);
    }

    try {
      conn.tableOperations().addSplits(table, splits);
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new IllegalStateException("Failed to add splits to Accumulo table " + table, e);
    }
  }

  public void setFluoTableSplits(SortedSet<Text> splits) {
    final String table = fluoConfig.getAccumuloTable();
    try {
      conn.tableOperations().addSplits(table, splits);
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new IllegalStateException("Failed to add splits to Fluo's Accumulo table " + table, e);
    }
  }

  public void makeHdfsTempDirs() {
    Path tempDir = new Path(dataConfig.hdfsTempDir);
    try {
      if (!hdfs.exists(tempDir)) {
        hdfs.mkdirs(tempDir);
      }
      if (!hdfs.exists(failuresDir)) {
        hdfs.mkdirs(failuresDir);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create HDFS temp dirs", e);
    }
  }

  public void saveToFluo(JavaPairRDD<Key, Value> data) throws Exception {
    Job job = Job.getInstance(sparkCtx.hadoopConfiguration());

    if (hdfs.exists(fluoTempDir)) {
      hdfs.delete(fluoTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(job, fluoTempDir);

    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(fluoTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, job.getConfiguration());
    conn.tableOperations().importDirectory(fluoConfig.getAccumuloTable(), fluoTempDir.toString(),
        failuresDir.toString(), false);
    log.info("Imported data at {} into Fluo app {}", fluoTempDir, fluoConfig.getApplicationName());
  }

  public void saveRowColBytesToAccumulo(JavaPairRDD<RowColumn, Bytes> rowColBytes) throws Exception {
    JavaPairRDD<Key, Value> kvData =
        rowColBytes.mapToPair(t -> new Tuple2<Key, Value>(SpanUtil.toKey(t._1()), new Value(t._2()
            .toArray())));
    saveKeyValueToAccumulo(kvData);
  }

  public void saveKeyValueToAccumulo(JavaPairRDD<Key, Value> data) throws Exception {
    Job accJob = Job.getInstance(sparkCtx.hadoopConfiguration());

    if (hdfs.exists(accumuloTempDir)) {
      hdfs.delete(accumuloTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(accJob, accumuloTempDir);
    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(accumuloTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, accJob.getConfiguration());
    conn.tableOperations().importDirectory(dataConfig.accumuloIndexTable,
        accumuloTempDir.toString(), failuresDir.toString(), false);
  }

  public FileSystem getHdfs() {
    return hdfs;
  }

  public Path getFluoTempDir() {
    return fluoTempDir;
  }

  public Path getAccumuloTempDir() {
    return accumuloTempDir;
  }

  public Path getHadoopTempDir() {
    return hadoopTempDir;
  }

  public Path getFailuresDir() {
    return failuresDir;
  }

  public JavaSparkContext getSparkCtx() {
    return sparkCtx;
  }

  public Connector getAccumuloConnector() {
    return conn;
  }

  public FluoConfiguration getFluoConfig() {
    return fluoConfig;
  }
}
