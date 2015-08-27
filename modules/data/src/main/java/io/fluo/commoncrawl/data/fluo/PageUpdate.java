package io.fluo.commoncrawl.data.fluo;

import java.net.MalformedURLException;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.fluo.api.client.Loader;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.commoncrawl.core.DataUtil;
import io.fluo.commoncrawl.core.models.Page;
import io.fluo.commoncrawl.data.util.FluoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageUpdate implements Loader {

  private static final Logger log = LoggerFactory.getLogger(PageUpdate.class);
  private Action action;
  private Page page;
  private String delUri;

  private PageUpdate() {
  }

  public static PageUpdate updatePage(Page page) {
    Preconditions.checkArgument(!page.isEmpty(), "Page cannot be empty");
    PageUpdate update = new PageUpdate();
    update.action = Action.UPDATE;
    update.page = page;
    return update;
  }

  public static PageUpdate deletePage(String url) throws MalformedURLException {
    Preconditions.checkArgument(!url.isEmpty(), "Url cannot be empty");
    PageUpdate update = new PageUpdate();
    update.action = Action.DELETE;
    update.delUri = DataUtil.toUri(url);
    return update;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);
    String row;

    switch (action) {
      case DELETE:
        ttx.mutate().row("p:" + delUri).col(FluoConstants.PAGE_NEW_COL).set("delete");
        break;
      case UPDATE:
        Gson gson = new Gson();
        String newJson = gson.toJson(page);
        ttx.mutate().row("p:" + page.getUri()).col(FluoConstants.PAGE_NEW_COL).set(newJson);
        break;
      default:
        log.error("PageUpdate called with no action");
    }
  }

  private enum Action {
    UPDATE,
    DELETE,
  }
}
