/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

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
