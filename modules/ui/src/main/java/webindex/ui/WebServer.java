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

package webindex.ui;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.gson.Gson;
import freemarker.template.Configuration;
import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.util.AccumuloUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.ModelAndView;
import spark.Request;
import spark.Spark;
import spark.template.freemarker.FreeMarkerEngine;
import webindex.core.IndexClient;
import webindex.core.WebIndexConfig;
import webindex.core.models.Links;
import webindex.core.models.Page;
import webindex.core.models.Pages;
import webindex.core.models.TopResults;

import static spark.Spark.get;
import static spark.Spark.halt;
import static spark.Spark.staticFiles;

public class WebServer {

  private static final Logger log = LoggerFactory.getLogger(WebServer.class);

  private IndexClient client;

  public WebServer() {}

  private TopResults getTop(Request req) {
    String next = Optional.ofNullable(req.queryParams("next")).orElse("");
    Integer pageNum = Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
    return client.getTopResults(next, pageNum);
  }

  private Page getPage(Request req) {
    String url = req.queryParams("url");
    if (url == null) {
      halt(400, "Bad request: url parameter was not set");
    }
    return client.getPage(url);
  }

  private Pages getPages(Request req) {
    String domain = req.queryParams("domain");
    if (domain == null) {
      halt(400, "Bad request: domain parameter was not set");
    }
    String next = Optional.ofNullable(req.queryParams("next")).orElse("");
    Integer pageNum = Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
    return client.getPages(domain, next, pageNum);
  }

  private Links getLinks(Request req) {
    String rawUrl = req.queryParams("url");
    if (rawUrl == null) {
      halt(400, "Bad request: url parameter was not set");
    }
    String linkType = req.queryParams("linkType");
    if (linkType == null) {
      halt(400, "Bad request: linkType parameter was not set");
    }
    String next = Optional.ofNullable(req.queryParams("next")).orElse("");
    Integer pageNum = Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
    return client.getLinks(rawUrl, linkType, next, pageNum);
  }

  public void start(IndexClient client, int port, Path templatePath) {
    this.client = client;

    Spark.port(port);

    staticFiles.location("/assets");

    FreeMarkerEngine freeMarkerEngine = new FreeMarkerEngine();
    if (templatePath != null && Files.exists(templatePath)) {
      log.info("Serving freemarker templates from {}", templatePath.toAbsolutePath());
      Configuration freeMarkerConfig = new Configuration();
      try {
        freeMarkerConfig.setDirectoryForTemplateLoading(templatePath.toFile());
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      freeMarkerEngine.setConfiguration(freeMarkerConfig);
    }

    get("/", (req, res) -> new ModelAndView(null, "home.ftl"), freeMarkerEngine);

    get("/top", (req, res) -> new ModelAndView(Collections.singletonMap("top", getTop(req)),
        "top.ftl"), freeMarkerEngine);

    Gson gson = new Gson();
    get("/api/top", (req, res) -> getTop(req), gson::toJson);

    get("/page", (req, res) -> new ModelAndView(Collections.singletonMap("page", getPage(req)),
        "page.ftl"), freeMarkerEngine);
    get("/api/page", (req, res) -> getPage(req), gson::toJson);

    get("/pages", (req, res) -> new ModelAndView(Collections.singletonMap("pages", getPages(req)),
        "pages.ftl"), freeMarkerEngine);
    get("/api/pages", (req, res) -> getPages(req), gson::toJson);

    get("/links", (req, res) -> new ModelAndView(Collections.singletonMap("links", getLinks(req)),
        "links.ftl"), freeMarkerEngine);
    get("/api/links", (req, res) -> getLinks(req), gson::toJson);
  }

  public void stop() {
    Spark.stop();
  }

  public static void main(String[] args) throws Exception {
    WebIndexConfig webIndexConfig = WebIndexConfig.load();
    File connPropsFile = new File(webIndexConfig.getConnPropsPath());
    FluoConfiguration fluoConfig = new FluoConfiguration(connPropsFile);
    fluoConfig.setApplicationName(webIndexConfig.fluoApp);
    try (FluoAdmin admin = FluoFactory.newAdmin(fluoConfig)) {
      for (Map.Entry<String, String> entry : admin.getApplicationConfig().toMap().entrySet()) {
        fluoConfig.setProperty(entry.getKey(), entry.getValue());
      }
    }
    Connector conn = AccumuloUtil.getConnector(fluoConfig);
    IndexClient client = new IndexClient(webIndexConfig.accumuloIndexTable, conn);
    WebServer webServer = new WebServer();
    webServer.start(client, 4567, null);
  }
}
