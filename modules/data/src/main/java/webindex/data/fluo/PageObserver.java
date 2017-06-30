/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.data.fluo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.data.RowHasher;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.types.TypedSnapshotBase.Value;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.Constants;
import webindex.core.models.Link;
import webindex.core.models.Page;
import webindex.core.models.UriInfo;
import webindex.core.models.export.IndexUpdate;
import webindex.core.models.export.PageUpdate;

public class PageObserver implements Observer {

  private static final Logger log = LoggerFactory.getLogger(PageObserver.class);
  private static final Gson gson = new Gson();

  private CombineQueue<String, UriInfo> uriQ;
  private ExportQueue<String, IndexUpdate> exportQ;
  private Meter pagesIngested;
  private Meter linksIngested;
  private Meter pagesChanged;

  private static final RowHasher PAGE_ROW_HASHER = new RowHasher("p");

  public static RowHasher getPageRowHasher() {
    return PAGE_ROW_HASHER;
  }

  PageObserver(CombineQueue<String, UriInfo> uriQ, ExportQueue<String, IndexUpdate> exportQ,
      MetricsReporter reporter) {
    this.uriQ = uriQ;
    this.exportQ = exportQ;
    pagesIngested = reporter.meter("webindex_pages_ingested");
    linksIngested = reporter.meter("webindex_links_ingested");
    pagesChanged = reporter.meter("webindex_pages_changed");

  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

    TypedTransactionBase ttx = Constants.TYPEL.wrap(tx);

    Map<Column, Value> pages =
        ttx.get().row(row).columns(Constants.PAGE_NEW_COL, Constants.PAGE_CUR_COL);

    String nextJson = pages.get(Constants.PAGE_NEW_COL).toString("");
    if (nextJson.isEmpty()) {
      log.error("An empty page was set at row {} col {}", row.toString(), col.toString());
      return;
    }

    Page curPage = Page.fromJson(gson, pages.get(Constants.PAGE_CUR_COL).toString(""));
    Set<Link> curLinks = curPage.getOutboundLinks();

    Map<String, UriInfo> updates = new HashMap<>();
    String pageUri = getPageRowHasher().removeHash(row).toString();

    Page nextPage = Page.fromJson(gson, nextJson);
    if (nextPage.isDelete()) {
      ttx.mutate().row(row).col(Constants.PAGE_CUR_COL).delete();
      updates.put(pageUri, new UriInfo(0, -1));
    } else {
      ttx.mutate().row(row).col(Constants.PAGE_CUR_COL).set(nextJson);
      if (curPage.isEmpty()) {
        updates.put(pageUri, new UriInfo(0, 1));
      }
      pagesIngested.mark();
    }

    Set<Link> nextLinks = nextPage.getOutboundLinks();

    List<Link> addLinks = new ArrayList<>(Sets.difference(nextLinks, curLinks));
    for (Link link : addLinks) {
      updates.put(link.getUri(), new UriInfo(1, 0));
    }
    linksIngested.mark(addLinks.size());

    List<Link> delLinks = new ArrayList<>(Sets.difference(curLinks, nextLinks));
    for (Link link : delLinks) {
      updates.put(link.getUri(), new UriInfo(-1, 0));
    }

    uriQ.addAll(tx, updates);

    exportQ.add(tx, pageUri, new PageUpdate(pageUri, nextJson, addLinks, delLinks));
    pagesChanged.mark();

    // clean up
    ttx.mutate().row(row).col(Constants.PAGE_NEW_COL).delete();
  }
}
