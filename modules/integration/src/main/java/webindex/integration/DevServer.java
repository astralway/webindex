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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.gson.Gson;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
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
  private MiniFluo miniFluo;
  private WebServer webServer;
  private IndexClient client;
  private AtomicBoolean running = new AtomicBoolean(false);
  private Path baseDir;
  private boolean metrics;

  public DevServer(Path dataPath, int webPort, Path templatePath, Path baseDir, boolean metrics) {
    this.dataPath = dataPath;
    this.webPort = webPort;
    this.templatePath = templatePath;
    this.baseDir = baseDir;
    this.metrics = metrics;
    this.webServer = new WebServer();
  }

  public IndexClient getIndexClient() {
    if (!running.get()) {
      throw new IllegalStateException("DevServer must be running before retrieving index client");
    }
    return client;
  }

  public SimpleConfiguration configureMetrics(SimpleConfiguration config) {
    if (metrics) {
      config.setProperty("fluo.metrics.reporter.graphite.enable", true);
      config.setProperty("fluo.metrics.reporter.graphite.host", "localhost");
      config.setProperty("fluo.metrics.reporter.graphite.port", 2003);
      config.setProperty("fluo.metrics.reporter.graphite.frequency", 30);
    }
    return config;
  }

  public void start() throws Exception {
    log.info("Starting WebIndex development server...");

    log.info("Starting MiniAccumuloCluster at {}", baseDir);

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(baseDir.toFile(), "secret");
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

    FluoConfiguration config = new FluoConfiguration();
    AccumuloExportITBase.configureFromMAC(config, cluster);
    config.setApplicationName("webindex");
    config.setAccumuloTable("webindex");
    configureMetrics(config);

    String exportTable = "webindex_search";

    log.info("Initializing Accumulo & Fluo");
    IndexEnv env = new IndexEnv(config, exportTable, "/tmp", TEST_SPLITS, TEST_SPLITS);
    env.initAccumuloIndexTable();
    env.configureApplication(config, config);

    FluoFactory.newAdmin(config).initialize(
        new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true));

    env.setFluoTableSplits();

    log.info("Starting web server");
    client = new IndexClient(exportTable, cluster.getConnector("root", "secret"));
    webServer.start(client, webPort, templatePath);

    log.info("Loading data from {}", dataPath);
    Gson gson = new Gson();
    miniFluo = FluoFactory.newMiniFluo(config);

    running.set(true);

    try (FluoClient client =
        FluoFactory.newClient(configureMetrics(miniFluo.getClientConfiguration()))) {

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
  }

  public void stop() {
    miniFluo.close();
    webServer.stop();
    try {
      cluster.stop();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static void main(String[] args) throws Exception {

    DevServerOpts opts = new DevServerOpts();
    JCommander commander = new JCommander(opts);
    commander.setProgramName("webindex dev");
    try {
      commander.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage() + "\n");
      commander.usage();
      System.exit(1);
    }

    if (opts.help) {
      commander.usage();
      System.exit(1);
    }

    Path dataPath = Paths.get(String.format("data/%d-pages.txt", opts.numPages));
    if (Files.notExists(dataPath)) {
      log.info("Generating sample data at {} for dev server", dataPath);
      SampleData.generate(dataPath, opts.numPages);
    }
    log.info("Loading data at {}", dataPath);

    Path templatePath = Paths.get(opts.templateDir);
    if (Files.notExists(templatePath)) {
      log.info("Template location {} does not exits", templatePath);
      throw new IllegalArgumentException("Template location does not exist");
    }

    Path baseDir = Files.createTempDirectory(Paths.get("target"), "webindex-dev-");
    DevServer devServer = new DevServer(dataPath, 4567, templatePath, baseDir, opts.metrics);
    devServer.start();
  }
}
