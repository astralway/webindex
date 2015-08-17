package io.fluo.commoncrawl.data.fluo;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.commoncrawl.core.Page;
import io.fluo.commoncrawl.data.util.FluoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageObserver extends AbstractObserver {

  private static final Logger log = LoggerFactory.getLogger(PageObserver.class);

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);
    String nextJson = ttx.get().row(row).col(FluoConstants.PAGE_NEW_COL).toString("");
    if (nextJson.isEmpty()) {
      log.error("An empty page was set at row {} col {}", row.toString(), col.toString());
      return;
    }

    Gson gson = new Gson();
    Page nextPage;
    if (nextJson.equals("delete")) {
      ttx.mutate().row(row).col(FluoConstants.PAGE_CUR_COL).delete();
      nextPage = Page.EMPTY;
    } else {
      ttx.mutate().row(row).col(FluoConstants.PAGE_CUR_COL).set(nextJson);
      nextPage = gson.fromJson(nextJson, Page.class);
    }

    String curJson = ttx.get().row(row).col(FluoConstants.PAGE_CUR_COL).toString("");
    Set<Page.Link> curLinks = Collections.emptySet();
    if (!curJson.isEmpty()) {
      Page curPage = gson.fromJson(curJson, Page.class);
      curLinks = curPage.getExternalLinks();
    }

    Set<Page.Link> nextLinks = nextPage.getExternalLinks();
    String pageUri = row.toString().substring(2);

    Sets.SetView<Page.Link> addLinks = Sets.difference(nextLinks, curLinks);
    for (Page.Link link : addLinks) {
      String r = "p:" + link.getUri();
      ttx.mutate().row(r).fam(FluoConstants.INLINKS_UPDATE).qual(pageUri)
          .set("add," + link.getAnchorText());
      ttx.mutate().row(r).col(FluoConstants.INLINKS_CHG_NTFY).weaklyNotify();
    }

    Sets.SetView<Page.Link> delLinks = Sets.difference(curLinks, nextLinks);
    for (Page.Link link : delLinks) {
      String r = "p:" + link.getUri();
      ttx.mutate().row(r).fam(FluoConstants.INLINKS_UPDATE).qual(pageUri).set("del");
      ttx.mutate().row(r).col(FluoConstants.INLINKS_CHG_NTFY).weaklyNotify();
    }

    // clean up
    ttx.mutate().row(row).col(FluoConstants.PAGE_NEW_COL).delete();
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(FluoConstants.PAGE_NEW_COL, NotificationType.STRONG);
  }
}
