package io.fluo.webindex.ui;

import java.io.File;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.webindex.core.DataConfig;
import io.fluo.core.util.AccumuloUtil;
import org.apache.accumulo.core.client.Connector;

public class WebIndexApp extends Application<WebIndexConfig> {

  public static void main(String[] args) throws Exception {
    new WebIndexApp().run(args);
  }

  @Override
  public String getName() {
    return "webindex-app";
  }

  @Override
  public void initialize(Bootstrap<WebIndexConfig> bootstrap) {
    bootstrap.addBundle(new ViewBundle<WebIndexConfig>());
  }

  @Override
  public void run(WebIndexConfig config, Environment environment) {

    DataConfig dataConfig = config.getDataConfig();
    File fluoConfigFile = new File(dataConfig.getFluoPropsPath());
    FluoConfiguration fluoConfig = new FluoConfiguration(fluoConfigFile);

    Connector conn = AccumuloUtil.getConnector(fluoConfig);
    final WebIndexResources resource = new WebIndexResources(fluoConfig, conn, dataConfig);
    environment.healthChecks().register("fluo", new FluoHealthCheck());
    environment.jersey().register(resource);
  }
}
