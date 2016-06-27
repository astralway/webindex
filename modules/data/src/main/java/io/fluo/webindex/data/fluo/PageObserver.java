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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.fluo.webindex.core.models.Link;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.FluoApp;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.util.FluoConstants;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.types.TypedSnapshotBase.Value;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.recipes.accumulo.export.AccumuloExport;
import org.apache.fluo.recipes.data.RowHasher;
import org.apache.fluo.recipes.export.ExportQueue;
import org.apache.fluo.recipes.map.CollisionFreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageObserver extends AbstractObserver {

  private static final Logger log = LoggerFactory.getLogger(PageObserver.class);
  private static final Gson gson = new Gson();

  private CollisionFreeMap<String, UriInfo> uriMap;
  private ExportQueue<String, AccumuloExport<String>> exportQ;

  private static final RowHasher PAGE_ROW_HASHER = new RowHasher("p");

  public static RowHasher getPageRowHasher() {
    return PAGE_ROW_HASHER;
  }

  @Override
  public void init(Context context) throws Exception {
    exportQ = ExportQueue.getInstance(FluoApp.EXPORT_QUEUE_ID, context.getAppConfiguration());
    uriMap = CollisionFreeMap.getInstance(UriMap.URI_MAP_ID, context.getAppConfiguration());
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);

    Map<Column, Value> pages =
        ttx.get().row(row).columns(FluoConstants.PAGE_NEW_COL, FluoConstants.PAGE_CUR_COL);

    String nextJson = pages.get(FluoConstants.PAGE_NEW_COL).toString("");
    if (nextJson.isEmpty()) {
      log.error("An empty page was set at row {} col {}", row.toString(), col.toString());
      return;
    }

    Page curPage = Page.fromJson(gson, pages.get(FluoConstants.PAGE_CUR_COL).toString(""));
    Set<Link> curLinks = curPage.getOutboundLinks();

    Map<String, UriInfo> updates = new HashMap<>();
    String pageUri = getPageRowHasher().removeHash(row).toString();

    Page nextPage = Page.fromJson(gson, nextJson);
    if (nextPage.isDelete()) {
      ttx.mutate().row(row).col(FluoConstants.PAGE_CUR_COL).delete();
      updates.put(pageUri, new UriInfo(0, -1));
    } else {
      ttx.mutate().row(row).col(FluoConstants.PAGE_CUR_COL).set(nextJson);
      if (curPage.isEmpty()) {
        updates.put(pageUri, new UriInfo(0, 1));
      }
    }

    Set<Link> nextLinks = nextPage.getOutboundLinks();

    Sets.SetView<Link> addLinks = Sets.difference(nextLinks, curLinks);
    for (Link link : addLinks) {
      updates.put(link.getPageID(), new UriInfo(1, 0));
    }

    Sets.SetView<Link> delLinks = Sets.difference(curLinks, nextLinks);
    for (Link link : delLinks) {
      updates.put(link.getPageID(), new UriInfo(-1, 0));
    }

    uriMap.update(tx, updates);

    exportQ.add(tx, pageUri, new PageExport(nextJson, addLinks, delLinks));

    // clean up
    ttx.mutate().row(row).col(FluoConstants.PAGE_NEW_COL).delete();
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(FluoConstants.PAGE_NEW_COL, NotificationType.STRONG);
  }
}
