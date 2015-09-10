package io.fluo.webindex.ui.views;

import io.dropwizard.views.View;
import io.fluo.webindex.core.models.Links;

public class LinksView extends View {

  private final Links links;

  public LinksView(Links links) {
    super("links.ftl");
    this.links = links;
  }

  public Links getLinks() {
    return links;
  }
}
