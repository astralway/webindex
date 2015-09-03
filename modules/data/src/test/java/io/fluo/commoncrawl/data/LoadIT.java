package io.fluo.commoncrawl.data;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import io.fluo.commoncrawl.core.models.Page;
import io.fluo.commoncrawl.data.fluo.InlinksObserver;
import io.fluo.commoncrawl.data.fluo.PageObserver;
import io.fluo.commoncrawl.data.fluo.PageUpdate;
import io.fluo.commoncrawl.data.util.ArchiveUtil;
import org.apache.commons.io.FileUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadIT {

  private static final Logger log = LoggerFactory.getLogger(LoadIT.class);

  private MiniFluo miniFluo;

  @Before
  public void setUpFluo() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration props = new FluoConfiguration();
    props.setApplicationName("ccrawl");
    props.setWorkerThreads(5);
    props.setMiniDataDir("target/mini");
    props.setLoaderThreads(0);
    props.setLoaderQueueSize(0);

    List<ObserverConfiguration> config = new ArrayList<>();
    config.add(new ObserverConfiguration(PageObserver.class.getName()));
    config.add(new ObserverConfiguration(InlinksObserver.class.getName()));
    props.setObservers(config);

    miniFluo = FluoFactory.newMiniFluo(props);
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

      String url = "http://1000games.me/games/gametion/";
      log.info("Deleting page {}", url);
      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(PageUpdate.deletePage(url));
      }

      miniFluo.waitForObservers();
      dump(client);
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
}
