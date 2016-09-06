/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.integration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.IndexClient;
import webindex.core.models.Page;
import webindex.data.fluo.PageLoader;
import webindex.data.spark.IndexEnv;
import webindex.ui.WebServer;

public class DevServer {

  private static final Logger log = LoggerFactory.getLogger(DevServer.class);
  private static final int TEST_SPLITS = 119;

  private Path dataPath;
  private int webPort;
  private Path templatePath;
  private MiniAccumuloCluster cluster;
  private WebServer webServer;
  private IndexClient client;
  private AtomicBoolean started = new AtomicBoolean(false);
  private Path baseDir;

  public DevServer(Path dataPath, int webPort, Path templatePath, Path baseDir) {
    this.dataPath = dataPath;
    this.webPort = webPort;
    this.templatePath = templatePath;
    this.baseDir = baseDir;
    this.webServer = new WebServer();
  }

  public IndexClient getIndexClient() {
    if (!started.get()) {
      throw new IllegalStateException("DevServer must be started before retrieving index client");
    }
    return client;
  }

  public void start() throws Exception {
    log.info("Starting WebIndex development server...");

    log.info("Starting MiniAccumuloCluster at {}", baseDir);

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(baseDir.toFile(), "secret");
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

    FluoConfiguration config = new FluoConfiguration();
    AccumuloExportITBase.configureFromMAC(config, cluster);
    config.setApplicationName("webindex-dev");
    config.setAccumuloTable("webindex");

    String exportTable = "webindex_search";

    log.info("Initializing Accumulo & Fluo");
    IndexEnv env = new IndexEnv(config, exportTable, "/tmp", TEST_SPLITS, TEST_SPLITS);
    env.initAccumuloIndexTable();
    env.configureApplication(config);

    FluoFactory.newAdmin(config).initialize(
        new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true));

    env.setFluoTableSplits();

    log.info("Starting web server");
    client = new IndexClient(exportTable, cluster.getConnector("root", "secret"));
    webServer.start(client, webPort, templatePath);

    log.info("Loading data from {}", dataPath);
    Gson gson = new Gson();
    try (MiniFluo miniFluo = FluoFactory.newMiniFluo(config);
        FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      try (LoaderExecutor le = client.newLoaderExecutor()) {

        Files
            .lines(dataPath)
            .map(json -> Page.fromJson(gson, json))
            .forEach(
                page -> {
                  log.debug("Loading page {} with {} links", page.getUrl(), page.getOutboundLinks()
                      .size());
                  le.execute(PageLoader.updatePage(page));
                });
      }

      log.info("Finished loading data. Waiting for observers to finish...");
      miniFluo.waitForObservers();
      log.info("Observers finished");
    }

    started.set(true);
  }

  public void stop() {
    webServer.stop();
    try {
      cluster.stop();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    String dataLocation = "data/1K-pages.txt";
    String templateLocation = "modules/ui/src/main/resources/spark/template/freemarker";
    if (args.length == 2) {
      dataLocation = args[0];
      templateLocation = args[1];
    }
    log.info("Looking for data at {}", dataLocation);

    Path dataPath = Paths.get(dataLocation);
    if (Files.notExists(dataPath)) {
      log.info("Generating sample data at {} for dev server", dataPath);
      SampleData.generate(dataPath, 1000);
    }

    Path templatePath = Paths.get(templateLocation);
    if (Files.notExists(templatePath)) {
      log.info("Template location {} does not exits", templateLocation);
      throw new IllegalArgumentException("Template location does not exist");
    }

    DevServer devServer =
        new DevServer(dataPath, 4567, templatePath, Files.createTempDirectory("webindex-dev-"));
    devServer.start();
  }
}
