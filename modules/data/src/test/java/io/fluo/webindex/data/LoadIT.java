package io.fluo.webindex.data;

import java.io.File;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.fluo.InlinksObserver;
import io.fluo.webindex.data.fluo.IndexExporter;
import io.fluo.webindex.data.fluo.PageObserver;
import io.fluo.webindex.data.fluo.PageUpdate;
import io.fluo.webindex.data.util.ArchiveUtil;
import io.fluo.recipes.export.ExportQueueOptions;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadIT {

  private static final Logger log = LoggerFactory.getLogger(LoadIT.class);

  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static MiniFluo miniFluo;
  private static FluoConfiguration config;
  private static final PasswordToken password = new PasswordToken("secret");
  private static AtomicInteger tableCounter = new AtomicInteger(1);
  private String exportTable;

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
  public void setUpFluo() throws Exception {
    config = new FluoConfiguration();
    config.setMiniStartAccumulo(false);
    config.setApplicationName("lit");
    config.setAccumuloInstance(cluster.getInstanceName());
    config.setAccumuloUser("root");
    config.setAccumuloPassword("secret");
    config.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    config.setAccumuloZookeepers(cluster.getZooKeepers());
    config.setAccumuloTable("data" + tableCounter.getAndIncrement());
    config.setWorkerThreads(5);

    config.setObservers(Arrays.asList(new ObserverConfiguration(PageObserver.class.getName()),
        new ObserverConfiguration(InlinksObserver.class.getName()), new ObserverConfiguration(
            IndexExporter.class.getName())));

    new IndexExporter()
        .setConfiguration(config.getAppConfiguration(), new ExportQueueOptions(5, 5));

    // create and configure export table
    exportTable = "export" + tableCounter.getAndIncrement();
    cluster.getConnector("root", "secret").tableOperations().create(exportTable);
    AccumuloExporter.setExportTableInfo(config.getAppConfiguration(), IndexExporter.QUEUE_ID,
        new TableInfo(cluster.getInstanceName(), cluster.getZooKeepers(), "root", "secret",
            exportTable));

    FluoFactory.newAdmin(config).initialize(
        new FluoAdmin.InitOpts().setClearTable(true).setClearZookeeper(true));

    miniFluo = FluoFactory.newMiniFluo(config);
  }


  @After
  public void tearDownFluo() throws Exception {
    if (miniFluo != null) {
      miniFluo.close();
    }
  }

  @Test
  public void testLoad() throws Exception {

    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      ArchiveReader ar = WARCReaderFactory.get(new File("src/test/resources/wat-18.warc"));

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        Iterator<ArchiveRecord> records = ar.iterator();
        while (records.hasNext()) {
          try {
            ArchiveRecord r = records.next();
            Page p = ArchiveUtil.buildPage(r);
            if (p.isEmpty() || p.getOutboundLinks().isEmpty()) {
              continue;
            }
            log.info("Loading page {} with {} links", p.getUrl(), p.getOutboundLinks().size());
            le.execute(PageUpdate.updatePage(p));
          } catch (ParseException e) {
            log.debug("Parse exception occurred", e);
          }
        }
      }
      ar.close();
      miniFluo.waitForObservers();
      dump(client);
      dumpExportTable();

      String url = "http://1000games.me/games/gametion/";
      log.info("Deleting page {}", url);
      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(PageUpdate.deletePage(url));
      }

      miniFluo.waitForObservers();
      dump(client);
      dumpExportTable();
    }
  }

  private void dump(FluoClient client) throws Exception {
    try (Snapshot s = client.newSnapshot()) {
      RowIterator iter = s.get(new ScannerConfiguration());

      System.out.println("== snapshot start ==");
      while (iter.hasNext()) {
        Map.Entry<Bytes, ColumnIterator> rowEntry = iter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext()) {
          Map.Entry<Column, Bytes> colEntry = citer.next();
          System.out.println(rowEntry.getKey() + " " + colEntry.getKey() + "\t"
              + colEntry.getValue());
        }
      }
      System.out.println("=== snapshot end ===");
    }
  }

  private void dumpExportTable() throws Exception {
    Connector conn = cluster.getConnector("root", "secret");
    Scanner scanner = conn.createScanner(exportTable, Authorizations.EMPTY);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

    System.out.println("== export table start ==");
    while (iterator.hasNext()) {
      Map.Entry<Key, Value> entry = iterator.next();
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
    System.out.println("== export table end ==");

  }
}
