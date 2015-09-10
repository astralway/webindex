package io.fluo.webindex.ui.views;

import io.dropwizard.views.View;
import io.fluo.webindex.core.models.TopResults;

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
