package io.fluo.commoncrawl.web.views;

import io.dropwizard.views.View;
import io.fluo.commoncrawl.web.models.PageStats;

public class PageView extends View {

  private final PageStats page;

  public PageView(PageStats page) {
    super("page.ftl");
    this.page = page;
  }

  public PageStats getPage() {
    return page;
  }
}
