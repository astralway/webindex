package io.fluo.commoncrawl.web;

import java.io.File;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.core.util.AccumuloUtil;
import org.apache.accumulo.core.client.Connector;

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
    bootstrap.addBundle(new ViewBundle<InboundConfiguration>());
  }

  @Override
  public void run(InboundConfiguration config,
                  Environment environment) {

    DataConfig dataConfig = config.getDataConfig();
    File fluoConfigFile = new File(dataConfig.fluoPropsPath);
    FluoConfiguration fluoConfig = new FluoConfiguration(fluoConfigFile);

    Connector conn = AccumuloUtil.getConnector(fluoConfig);
    final InboundResource resource = new InboundResource(fluoConfig, conn, dataConfig);
    environment.healthChecks().register("fluo", new FluoHealthCheck());
    environment.jersey().register(resource);
  }
}
