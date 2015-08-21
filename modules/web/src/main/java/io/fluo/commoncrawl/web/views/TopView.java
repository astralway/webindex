package io.fluo.commoncrawl.web.views;

import io.dropwizard.views.View;
import io.fluo.commoncrawl.core.models.TopResults;

public class TopView extends View {

  private TopResults top;

  public TopView(TopResults results) {
    super("top.ftl");
    this.top = results;
  }

  public TopResults getTop() {
    return top;
  }
}
