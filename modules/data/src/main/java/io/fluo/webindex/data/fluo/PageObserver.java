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
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.FluoApp;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.recipes.Transmutable;
import io.fluo.webindex.data.util.FluoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageObserver extends AbstractObserver {

  private static final Logger log = LoggerFactory.getLogger(PageObserver.class);
  private static final Gson gson = new Gson();

  private CollisionFreeMap<String, UriInfo, UriInfo> uriMap;
  private ExportQueue<String, Transmutable<String>> exportQ;

  @Override
  public void init(Context context) throws Exception {
    exportQ = ExportQueue.getInstance(FluoApp.EXPORT_QUEUE_ID, context.getAppConfiguration());
    uriMap = CollisionFreeMap.getInstance(UriMap.URI_MAP_ID, context.getAppConfiguration());
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);

    String nextJson = ttx.get().row(row).col(FluoConstants.PAGE_NEW_COL).toString("");
    if (nextJson.isEmpty()) {
      log.error("An empty page was set at row {} col {}", row.toString(), col.toString());
      return;
    }

    Page curPage =
        Page.fromJson(gson, ttx.get().row(row).col(FluoConstants.PAGE_CUR_COL).toString(""));
    Set<Page.Link> curLinks = curPage.getOutboundLinks();

    Map<String, UriInfo> updates = new HashMap<>();
    String pageUri = row.toString().substring(2);

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

    Set<Page.Link> nextLinks = nextPage.getOutboundLinks();

    Sets.SetView<Page.Link> addLinks = Sets.difference(nextLinks, curLinks);
    for (Page.Link link : addLinks) {
      updates.put(link.getUri(), new UriInfo(1, 0));
    }

    Sets.SetView<Page.Link> delLinks = Sets.difference(curLinks, nextLinks);
    for (Page.Link link : delLinks) {
      updates.put(link.getUri(), new UriInfo(-1, 0));
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
