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
import java.util.Optional;

import freemarker.template.Configuration;
import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.util.AccumuloUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.ModelAndView;
import spark.Spark;
import spark.template.freemarker.FreeMarkerEngine;
import webindex.core.IndexClient;
import webindex.core.WebIndexConfig;
import webindex.core.models.Links;
import webindex.core.models.Page;
import webindex.core.models.Pages;
import webindex.core.models.TopResults;

import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class WebServer {

  private static final Logger log = LoggerFactory.getLogger(WebServer.class);

  private static final ModelAndView VIEW_404 = new ModelAndView(null, "404.ftl");

  public WebServer() {}

  public void start(IndexClient client, int port, Path templatePath) {

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

    get("/top",
        (req, res) -> {
          String next = Optional.ofNullable(req.queryParams("next")).orElse("");
          Integer pageNum =
              Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
          TopResults results = client.getTopResults(next, pageNum);
          return new ModelAndView(Collections.singletonMap("top", results), "top.ftl");
        }, freeMarkerEngine);

    get("/page", (req, res) -> {
      if (req.queryParams("url") == null) {
        return VIEW_404;
      }
      String url = req.queryParams("url");
      Page page = client.getPage(url);
      return new ModelAndView(Collections.singletonMap("page", page), "page.ftl");
    }, freeMarkerEngine);

    get("/pages",
        (req, res) -> {
          if (req.queryParams("domain") == null) {
            return VIEW_404;
          }
          String domain = req.queryParams("domain");
          String next = Optional.ofNullable(req.queryParams("next")).orElse("");
          Integer pageNum =
              Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
          Pages pages = client.getPages(domain, next, pageNum);
          return new ModelAndView(Collections.singletonMap("pages", pages), "pages.ftl");
        }, freeMarkerEngine);

    get("/links",
        (req, res) -> {
          if (req.queryParams("url") == null || req.queryParams("linkType") == null) {
            return VIEW_404;
          }
          String rawUrl = req.queryParams("url");
          String linkType = req.queryParams("linkType");
          String next = Optional.ofNullable(req.queryParams("next")).orElse("");
          Integer pageNum =
              Integer.parseInt(Optional.ofNullable(req.queryParams("pageNum")).orElse("0"));
          Links links = client.getLinks(rawUrl, linkType, next, pageNum);
          return new ModelAndView(Collections.singletonMap("links", links), "links.ftl");
        }, freeMarkerEngine);
  }

  public void stop() {
    Spark.stop();
  }

  public static void main(String[] args) throws Exception {
    WebIndexConfig webIndexConfig = WebIndexConfig.load();
    File fluoConfigFile = new File(webIndexConfig.getFluoPropsPath());
    FluoConfiguration fluoConfig = new FluoConfiguration(fluoConfigFile);
    Connector conn = AccumuloUtil.getConnector(fluoConfig);
    IndexClient client = new IndexClient(webIndexConfig.accumuloIndexTable, conn);
    WebServer webServer = new WebServer();
    webServer.start(client, 4567, null);
  }
}
