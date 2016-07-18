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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.FluoApp;
import io.fluo.webindex.data.fluo.PageObserver;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.recipes.accumulo.export.TableInfo;
import org.apache.fluo.recipes.accumulo.ops.TableOperations;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.spark.FluoSparkHelper;
import org.apache.fluo.recipes.spark.FluoSparkHelper.BulkImportOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexEnv {

  private static final Logger log = LoggerFactory.getLogger(IndexEnv.class);

  private final String accumuloTable;
  private Connector conn;
  private FluoConfiguration fluoConfig;
  private Path accumuloTempDir;
  private Path fluoTempDir;
  private int numTablets;
  private int numBuckets;

  public IndexEnv(DataConfig dataConfig) {
    this(getFluoConfig(dataConfig), dataConfig.accumuloIndexTable, dataConfig.hdfsTempDir,
        dataConfig.numBuckets, dataConfig.numTablets);
  }

  public IndexEnv(FluoConfiguration fluoConfig, String accumuloTable, String hdfsTempDir,
      int numBuckets, int numTablets) {
    this.fluoConfig = fluoConfig;
    this.accumuloTable = accumuloTable;
    this.numBuckets = numBuckets;
    this.numTablets = numTablets;
    conn = AccumuloUtil.getConnector(fluoConfig);
    fluoTempDir = new Path(hdfsTempDir + "/fluo");
    accumuloTempDir = new Path(hdfsTempDir + "/accumulo");
  }

  public static String getHadoopConfDir() {
    final String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir == null) {
      log.error("HADOOP_CONF_DIR must be set in environment!");
      System.exit(1);
    }
    if (!(new File(hadoopConfDir).exists())) {
      log.error("Directory set by HADOOP_CONF_DIR={} does not exist", hadoopConfDir);
      System.exit(1);
    }
    return hadoopConfDir;
  }

  private static FluoConfiguration getFluoConfig(DataConfig dataConfig) {
    Preconditions.checkArgument(new File(dataConfig.getFluoPropsPath()).exists(),
        "fluoPropsPath must be set in data.yml and exist");
    return new FluoConfiguration(new File(dataConfig.getFluoPropsPath()));
  }

  public FluoConfiguration getFluoConfig() {
    return fluoConfig;
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

  public static FileSystem getHDFS() throws IOException {
    return getHDFS(getHadoopConfDir());
  }

  public static FileSystem getHDFS(String hadoopConfDir) throws IOException {
    Configuration config = new Configuration();
    config.addResource(hadoopConfDir);
    return FileSystem.get(config);
  }

  public static void validateDataDir(String dataDir) {
    try {
      FileSystem hdfs = getHDFS();
      Path dataPath = new Path(dataDir);
      if (!hdfs.exists(dataPath)) {
        log.error("HDFS data directory {} does not exist", dataDir);
        System.exit(-1);
      }
      RemoteIterator<LocatedFileStatus> listIter = hdfs.listFiles(dataPath, true);
      while (listIter.hasNext()) {
        LocatedFileStatus status = listIter.next();
        if (status.isFile()) {
          return;
        }
      }
      log.error("HDFS data directory {} has no files", dataDir);
      System.exit(-1);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void initAccumuloIndexTable() {
    if (conn.tableOperations().exists(accumuloTable)) {
      try {
        conn.tableOperations().delete(accumuloTable);
      } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
        throw new IllegalStateException("Failed to delete Accumulo table " + accumuloTable, e);
      }
    }
    try {
      conn.tableOperations().create(accumuloTable);
    } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
      throw new IllegalStateException("Failed to create Accumulo table " + accumuloTable, e);
    }

    try {
      conn.tableOperations().addSplits(accumuloTable, IndexEnv.getAccumuloDefaultSplits());
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new IllegalStateException("Failed to add splits to Accumulo table " + accumuloTable, e);
    }
  }

  public void setFluoTableSplits() {
    final String table = fluoConfig.getAccumuloTable();
    try {
      TableOptimizations tableOptimizations =
          TableOptimizations.getConfiguredOptimizations(getFluoConfig());
      tableOptimizations.merge(PageObserver.getPageRowHasher().getTableOptimizations(numTablets));
      TableOperations.optimizeTable(getFluoConfig(), tableOptimizations);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to add splits to Fluo's Accumulo table " + table, e);
    }
  }

  public void configureApplication(FluoConfiguration appConfig) {
    FluoApp.configureApplication(appConfig,
        new TableInfo(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers(),
            fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword(), accumuloTable),
        numBuckets, numTablets);
  }

  public void initializeIndexes(JavaSparkContext ctx, JavaRDD<Page> pages, IndexStats stats)
      throws Exception {

    JavaPairRDD<String, UriInfo> uriMap = IndexUtil.createUriMap(pages);
    JavaPairRDD<String, Long> domainMap = IndexUtil.createDomainMap(uriMap);

    // Create the Accumulo index from pages RDD
    JavaPairRDD<RowColumn, Bytes> accumuloIndex =
        IndexUtil.createAccumuloIndex(stats, pages, uriMap, domainMap);

    // Create a Fluo index by filtering a subset of data from Accumulo index
    JavaPairRDD<RowColumn, Bytes> fluoIndex =
        IndexUtil.createFluoTable(pages, uriMap, domainMap, numBuckets);

    // Load the indexes into Fluo and Accumulo
    saveRowColBytesToFluo(ctx, fluoIndex);
    saveRowColBytesToAccumulo(ctx, accumuloIndex);
  }

  public void saveRowColBytesToFluo(JavaSparkContext ctx, JavaPairRDD<RowColumn, Bytes> data)
      throws Exception {
    new FluoSparkHelper(fluoConfig, ctx.hadoopConfiguration(), fluoTempDir).bulkImportRcvToFluo(
        data, new BulkImportOptions().setAccumuloConnector(conn));
  }

  public void saveRowColBytesToAccumulo(JavaSparkContext ctx, JavaPairRDD<RowColumn, Bytes> data)
      throws Exception {
    new FluoSparkHelper(fluoConfig, ctx.hadoopConfiguration(), accumuloTempDir)
        .bulkImportRcvToAccumulo(data, accumuloTable,
            new BulkImportOptions().setAccumuloConnector(conn));
  }

  public static List<String> getPathsRange(String ccPaths, String range) {
    if (!(new File(ccPaths).exists())) {
      log.error("CC paths file {} does not exist", ccPaths);
      System.exit(1);
    }
    int start = 0;
    int end = 0;
    try {
      start = Integer.parseInt(range.split("-")[0]);
      end = Integer.parseInt(range.split("-")[1]);
    } catch (NumberFormatException e) {
      log.error("Invalid range: {}", range);
      System.exit(1);
    }
    if (start > end) {
      log.error("Invalid range: {}", range);
      System.exit(1);
    }
    try (Stream<String> lines = Files.lines(Paths.get(ccPaths))) {
      return lines.skip(start).limit(end - start + 1).collect(Collectors.toList());
    } catch (IOException e) {
      log.error("Failed to read CC paths file {}", ccPaths, e);
      System.exit(1);
    }
    return Collections.emptyList();
  }
}
