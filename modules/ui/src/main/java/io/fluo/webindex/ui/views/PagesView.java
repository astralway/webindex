package io.fluo.webindex.ui.views;

import io.dropwizard.views.View;
import io.fluo.webindex.core.models.Pages;

public class PagesView extends View {

  private final Pages pages;

  public PagesView(Pages pages) {
    super("pages.ftl");
    this.pages = pages;
  }

  public Pages getPages() {
    return pages;
  }
}
