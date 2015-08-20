package io.fluo.commoncrawl.data;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.commoncrawl.core.models.Page;
import io.fluo.commoncrawl.data.fluo.InlinksObserver;
import io.fluo.commoncrawl.data.fluo.PageObserver;
import io.fluo.commoncrawl.data.fluo.PageUpdate;
import io.fluo.commoncrawl.data.util.ArchiveUtil;
import io.fluo.core.client.LoaderExecutorImpl;
import io.fluo.core.impl.Environment;
import io.fluo.integration.ITBaseMini;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadIT extends ITBaseMini {

  private static final Logger log = LoggerFactory.getLogger(LoadIT.class);

  @Override
  protected List<ObserverConfiguration> getObservers() {
    List<ObserverConfiguration> config = new ArrayList<>();
    config.add(new ObserverConfiguration(PageObserver.class.getName()));
    config.add(new ObserverConfiguration(InlinksObserver.class.getName()));
    return config;
  }

  @Test
  public void testLoad() throws Exception {

    config.setLoaderThreads(0);
    config.setLoaderQueueSize(0);

    try (Environment env = new Environment(config);
         LoaderExecutor le = new LoaderExecutorImpl(config, env)) {

      ArchiveReader ar = WARCReaderFactory.get(new File("src/test/resources/wat-18.warc"));

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
      ar.close();
      miniFluo.waitForObservers();
      printSnapshot();

      String url = "http://1000games.me/games/gametion/";
      log.info("Deleting page {}", url);
      le.execute(PageUpdate.deletePage(url));

      miniFluo.waitForObservers();
      printSnapshot();
    }
  }
}
