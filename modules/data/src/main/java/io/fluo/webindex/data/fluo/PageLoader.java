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

import java.net.MalformedURLException;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.core.models.URL;
import io.fluo.webindex.data.util.FluoConstants;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.recipes.data.RowHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageLoader implements Loader {

  private static final Logger log = LoggerFactory.getLogger(PageLoader.class);
  private Action action;
  private Page page;
  private URL delUrl;

  private PageLoader() {}

  public static PageLoader updatePage(Page page) {
    Preconditions.checkArgument(!page.isEmpty(), "Page cannot be empty");
    PageLoader update = new PageLoader();
    update.action = Action.UPDATE;
    update.page = page;
    return update;
  }

  public static PageLoader deletePage(URL url) throws MalformedURLException {
    Objects.requireNonNull(url, "Url cannot be null");
    PageLoader update = new PageLoader();
    update.action = Action.DELETE;
    update.delUrl = url;
    return update;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);

    Gson gson = new Gson();
    RowHasher rowHasher = PageObserver.getPageRowHasher();

    switch (action) {
      case DELETE:
        ttx.mutate().row(rowHasher.addHash(delUrl.toPageID())).col(FluoConstants.PAGE_NEW_COL)
            .set(Page.DELETE_JSON);
        break;
      case UPDATE:
        String newJson = gson.toJson(page);
        ttx.mutate().row(rowHasher.addHash(page.getPageID())).col(FluoConstants.PAGE_NEW_COL)
            .set(newJson);
        break;
      default:
        log.error("PageUpdate called with no action");
    }
  }

  private enum Action {
    UPDATE, DELETE,
  }
}
