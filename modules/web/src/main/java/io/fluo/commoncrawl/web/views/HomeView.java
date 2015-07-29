package io.fluo.commoncrawl.web.views;

import io.dropwizard.views.View;
import io.fluo.commoncrawl.web.models.Site;

public class HomeView extends View {

  public HomeView() {
    super("home.ftl");
  }
}
