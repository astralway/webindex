package io.fluo.webindex.ui.views;

import io.dropwizard.views.View;
import io.fluo.webindex.core.models.Page;

public class PageView extends View {

  private final Page page;

  public PageView(Page page) {
    super("page.ftl");
    this.page = page;
  }

  public Page getPage() {
    return page;
  }
}
