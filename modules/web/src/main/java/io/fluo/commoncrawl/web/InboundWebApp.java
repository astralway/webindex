package io.fluo.commoncrawl.web;

import java.io.File;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.fluo.api.config.FluoConfiguration;

public class InboundWebApp extends Application<InboundConfiguration> {

  public static void main(String[] args) throws Exception {
    new InboundWebApp().run(args);
  }

  @Override
  public String getName() {
    return "inbound-web-app";
  }

  @Override
  public void initialize(Bootstrap<InboundConfiguration> bootstrap) {
    // nothing to do ye
  }

  @Override
  public void run(InboundConfiguration config,
      Environment environment) {

    FluoConfiguration fluoConfig = new FluoConfiguration(new File(config.getFluoPropsPath()));
    final InboundResource resource = new InboundResource(fluoConfig);
    environment.healthChecks().register("fluo", new FluoHealthCheck());
    environment.jersey().register(resource);
  }
}
