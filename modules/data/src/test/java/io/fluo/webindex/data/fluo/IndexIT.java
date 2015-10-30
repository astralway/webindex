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

package io.fluo.webindex.data.fluo;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.FluoApp;
import io.fluo.webindex.data.SparkTestUtil;
import io.fluo.webindex.data.spark.Hex;
import io.fluo.webindex.data.spark.IndexEnv;
import io.fluo.webindex.data.spark.IndexStats;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.ArchiveUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class IndexIT {

  private static final Logger log = LoggerFactory.getLogger(IndexIT.class);

  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static MiniFluo miniFluo;
  private static final PasswordToken password = new PasswordToken("secret");
  private static AtomicInteger tableCounter = new AtomicInteger(1);
  private String exportTable;
  private transient JavaSparkContext ctx;
  private IndexEnv env;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg =
        new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), new String(password.getPassword()));
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Before
  public void setUp() throws Exception {
    FluoConfiguration config = new FluoConfiguration();
    config.setMiniStartAccumulo(false);
    config.setApplicationName("lit");
    config.setAccumuloInstance(cluster.getInstanceName());
    config.setAccumuloUser("root");
    config.setAccumuloPassword("secret");
    config.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    config.setAccumuloZookeepers(cluster.getZooKeepers());
    config.setAccumuloTable("data" + tableCounter.getAndIncrement());
    config.setWorkerThreads(5);

    // create and configure export table
    exportTable = "export" + tableCounter.getAndIncrement();

    ctx = SparkTestUtil.getSparkContext(getClass().getSimpleName());
    env = new IndexEnv(config, exportTable, ctx.hadoopConfiguration(), "/tmp");
    env.initAccumuloIndexTable();
    env.configureApplication(config);

    FluoFactory.newAdmin(config).initialize(
        new FluoAdmin.InitOpts().setClearTable(true).setClearZookeeper(true));

    env.setFluoTableSplits();

    miniFluo = FluoFactory.newMiniFluo(config);
  }

  @After
  public void tearDownFluo() throws Exception {
    ctx.stop();
    ctx = null;
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  public static Map<String, Page> readPages(File input) throws Exception {
    Map<String, Page> pageMap = new HashMap<>();
    ArchiveReader ar = WARCReaderFactory.get(input);
    for (ArchiveRecord r : ar) {
      Page p = ArchiveUtil.buildPage(r);
      if (p.isEmpty() || p.getOutboundLinks().isEmpty()) {
        continue;
      }
      pageMap.put(p.getUrl(), p);
    }
    ar.close();
    return pageMap;
  }

  private void assertOutput(Collection<Page> pages) throws Exception {
    JavaRDD<Page> pagesRDD = ctx.parallelize(new ArrayList<>(pages));
    Assert.assertEquals(pages.size(), pagesRDD.count());

    // Create expected output using spark
    IndexStats stats = new IndexStats(ctx);
    JavaPairRDD<RowColumn, Bytes> accumuloIndex = IndexUtil.createAccumuloIndex(stats, pagesRDD);
    JavaPairRDD<RowColumn, Bytes> fluoIndex =
        IndexUtil.createFluoIndex(accumuloIndex, FluoApp.NUM_BUCKETS);

    // Compare against actual
    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      boolean foundDiff = diffAccumuloTable(accumuloIndex.collect());
      foundDiff |= diffFluoTable(client, fluoIndex.collect());
      if (foundDiff) {
        printFluoTable(client);
        printAccumuloTable();
        printRDD(accumuloIndex.collect());
        printRDD(fluoIndex.collect());
      }
      Assert.assertFalse(foundDiff);
    }
  }

  @Test
  public void testFluoIndexing() throws Exception {

    Map<String, Page> pages = readPages(new File("src/test/resources/wat-18.warc"));

    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        for (Page page : pages.values()) {
          log.debug("Loading page {} with {} links", page.getUrl(), page.getOutboundLinks().size());
          le.execute(PageLoader.updatePage(page));
        }
      }

      miniFluo.waitForObservers();
      assertOutput(pages.values());

      String deleteUrl = "http://1000games.me/games/gametion/";
      log.info("Deleting page {}", deleteUrl);
      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(PageLoader.deletePage(deleteUrl));
      }
      miniFluo.waitForObservers();

      int numPages = pages.size();
      Assert.assertNotNull(pages.remove(deleteUrl));
      Assert.assertEquals(numPages - 1, pages.size());
      assertOutput(pages.values());

      String updateUrl = "http://100zone.blogspot.com/2013/03/please-memp3-4shared.html";
      Page updatePage = pages.get(updateUrl);
      long numLinks = updatePage.getNumOutbound();
      Assert.assertTrue(updatePage.addOutboundLink("http://example.com", "Example"));
      Assert.assertEquals(numLinks + 1, (long) updatePage.getNumOutbound());
      Assert.assertTrue(updatePage.removeOutboundLink("http://www.blogger.com"));
      Assert.assertEquals(numLinks, (long) updatePage.getNumOutbound());

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(PageLoader.updatePage(updatePage));
      }
      miniFluo.waitForObservers();

      Assert.assertNotNull(pages.put(updateUrl, updatePage));
      assertOutput(pages.values());
    }
  }

  @Test
  public void testSparkThenFluoIndexing() throws Exception {

    Map<String, Page> pageMap = readPages(new File("src/test/resources/wat-18.warc"));
    List<Page> pages = new ArrayList<>(pageMap.values());

    env.initializeIndexes(ctx, ctx.parallelize(pages.subList(0, 2)), new IndexStats(ctx));

    assertOutput(pages.subList(0, 2));

    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration());
        LoaderExecutor le = client.newLoaderExecutor()) {
      for (Page page : pages.subList(2, pages.size())) {
        log.info("Loading page {} with {} links {}", page.getUrl(), page.getOutboundLinks().size(),
            page.getOutboundLinks());
        le.execute(PageLoader.updatePage(page));
      }
    }
    miniFluo.waitForObservers();

    assertOutput(pages);
  }

  private void printRDD(List<Tuple2<RowColumn, Bytes>> rcvRDD) {
    System.out.println("== RDD start ==");
    rcvRDD.forEach(t -> System.out.println("rc " + Hex.encNonAscii(t, " ")));
    System.out.println("== RDD end ==");
  }

  private void printFluoTable(FluoClient client) throws Exception {
    try (Snapshot s = client.newSnapshot()) {
      RowIterator iter = s.get(new ScannerConfiguration());

      System.out.println("== fluo start ==");
      while (iter.hasNext()) {
        Map.Entry<Bytes, ColumnIterator> rowEntry = iter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext()) {
          Map.Entry<Column, Bytes> colEntry = citer.next();

          StringBuilder sb = new StringBuilder();
          Hex.encNonAscii(sb, rowEntry.getKey());
          sb.append(" ");
          Hex.encNonAscii(sb, colEntry.getKey(), " ");
          sb.append("\t");
          Hex.encNonAscii(sb, colEntry.getValue());

          System.out.println(sb.toString());
        }
      }
      System.out.println("=== fluo end ===");
    }
  }

  private boolean diffFluoTable(FluoClient client, List<Tuple2<RowColumn, Bytes>> linkIndex)
      throws Exception {
    try (Snapshot s = client.newSnapshot()) {
      RowIterator iter = s.get(new ScannerConfiguration());
      Iterator<Tuple2<RowColumn, Bytes>> indexIter = linkIndex.iterator();

      while (iter.hasNext()) {
        Map.Entry<Bytes, ColumnIterator> rowEntry = iter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext() && indexIter.hasNext()) {
          Map.Entry<Column, Bytes> colEntry = citer.next();
          Tuple2<RowColumn, Bytes> indexEntry = indexIter.next();
          RowColumn rc = indexEntry._1();
          Column col = colEntry.getKey();

          boolean retval = diff("fluo row", rc.getRow(), rowEntry.getKey());
          retval |= diff("fluo fam", rc.getColumn().getFamily(), col.getFamily());
          retval |= diff("fluo qual", rc.getColumn().getQualifier(), col.getQualifier());
          if (!col.getQualifier().toString().equals(Constants.CUR)) {
            retval |= diff("fluo val", indexEntry._2(), colEntry.getValue());
          }

          if (retval) {
            log.error("Difference found - row {} cf {} cq {} val {}", rc.getRow().toString(), rc
                .getColumn().getFamily().toString(), rc.getColumn().getQualifier().toString(),
                indexEntry._2().toString());
            return true;
          }

          log.debug("Verified {}", Hex.encNonAscii(indexEntry, " "));
        }
        if (citer.hasNext()) {
          log.error("An column iterator still has more data");
          return true;
        }
      }
      if (iter.hasNext() || indexIter.hasNext()) {
        log.error("An iterator still has more data");
        return true;
      }
      log.debug("No difference found");
      return false;
    }
  }

  private void printAccumuloTable() throws Exception {
    Connector conn = cluster.getConnector("root", "secret");
    Scanner scanner = conn.createScanner(exportTable, Authorizations.EMPTY);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

    System.out.println("== accumulo start ==");
    while (iterator.hasNext()) {
      Map.Entry<Key, Value> entry = iterator.next();
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
    System.out.println("== accumulo end ==");
  }

  private boolean diff(String dataType, String expected, String actual) {
    if (!expected.equals(actual)) {
      log.error("Difference found in {} - expected {} actual {}", dataType, expected, actual);
      return true;
    }
    return false;
  }

  private boolean diff(String dataType, Bytes expected, Bytes actual) {
    if (!expected.equals(actual)) {
      log.error("Difference found in {} - expected {} actual {}", dataType,
          Hex.encNonAscii(expected), Hex.encNonAscii(actual));
      return true;
    }
    return false;
  }

  private boolean diffAccumuloTable(List<Tuple2<RowColumn, Bytes>> linkIndex) throws Exception {
    Connector conn = cluster.getConnector("root", "secret");
    Scanner scanner = conn.createScanner(exportTable, Authorizations.EMPTY);
    Iterator<Map.Entry<Key, Value>> exportIter = scanner.iterator();
    Iterator<Tuple2<RowColumn, Bytes>> indexIter = linkIndex.iterator();

    while (exportIter.hasNext() && indexIter.hasNext()) {
      Tuple2<RowColumn, Bytes> indexEntry = indexIter.next();
      Map.Entry<Key, Value> exportEntry = exportIter.next();
      Key key = exportEntry.getKey();
      RowColumn rc = indexEntry._1();
      Column col = rc.getColumn();

      boolean retval = diff("row", rc.getRow().toString(), key.getRow().toString());
      retval |= diff("fam", col.getFamily().toString(), key.getColumnFamily().toString());
      retval |= diff("qual", col.getQualifier().toString(), key.getColumnQualifier().toString());
      if (!col.getQualifier().toString().equals(Constants.CUR)) {
        retval |= diff("val", indexEntry._2().toString(), exportEntry.getValue().toString());
      }

      if (retval) {
        log.error("Difference found - row {} cf {} cq {} val {}", rc.getRow().toString(), rc
            .getColumn().getFamily().toString(), rc.getColumn().getQualifier().toString(),
            indexEntry._2().toString());
        return true;
      }
      log.debug("Verified row {} cf {} cq {} val {}", rc.getRow().toString(), rc.getColumn()
          .getFamily().toString(), rc.getColumn().getQualifier().toString(), indexEntry._2()
          .toString());
    }

    if (exportIter.hasNext() || indexIter.hasNext()) {
      log.error("An iterator still has more data");
      return true;
    }

    log.debug("No difference found");
    return false;
  }
}
