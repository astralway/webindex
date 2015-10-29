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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.fluo.api.client.Loader;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.webindex.core.DataUtil;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.util.FluoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageLoader implements Loader {

  private static final Logger log = LoggerFactory.getLogger(PageLoader.class);
  private Action action;
  private Page page;
  private String delUri;

  private PageLoader() {}

  public static PageLoader updatePage(Page page) {
    Preconditions.checkArgument(!page.isEmpty(), "Page cannot be empty");
    PageLoader update = new PageLoader();
    update.action = Action.UPDATE;
    update.page = page;
    return update;
  }

  public static PageLoader deletePage(String url) throws MalformedURLException {
    Preconditions.checkArgument(!url.isEmpty(), "Url cannot be empty");
    PageLoader update = new PageLoader();
    update.action = Action.DELETE;
    update.delUri = DataUtil.toUri(url);
    return update;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);
    String row;

    Gson gson = new Gson();

    switch (action) {
      case DELETE:
        ttx.mutate().row("p:" + delUri).col(FluoConstants.PAGE_NEW_COL).set(Page.DELETE_JSON);
        break;
      case UPDATE:
        String newJson = gson.toJson(page);
        ttx.mutate().row("p:" + page.getUri()).col(FluoConstants.PAGE_NEW_COL).set(newJson);
        break;
      default:
        log.error("PageUpdate called with no action");
    }
  }

  private enum Action {
    UPDATE, DELETE,
  }
}
