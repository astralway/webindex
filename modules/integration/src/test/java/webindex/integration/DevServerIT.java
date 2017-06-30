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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import webindex.core.IndexClient;
import webindex.core.models.Pages;

public class DevServerIT {

  static DevServer devServer;
  static Path tempPath;

  @BeforeClass
  public static void init() throws Exception {
    tempPath = Files.createTempDirectory(Paths.get("target/"), "webindex-dev-");
    Path dataPath = Paths.get("src/test/resources/5-pages.txt");
    devServer = new DevServer(dataPath, 24567, null, tempPath, false);
    devServer.start();
  }

  @Test
  public void basic() throws Exception {
    Document doc = Jsoup.connect("http://localhost:24567/").get();
    Assert.assertTrue(doc.text().contains("Enter a domain to view known webpages in that domain"));

    IndexClient client = devServer.getIndexClient();
    Pages pages = client.getPages("stackoverflow.com", "", 0);
    Assert.assertEquals(4, pages.getTotal().intValue());

    Pages.PageScore pageScore = pages.getPages().get(0);
    Assert.assertEquals("http://blog.stackoverflow.com/2009/06/attribution-required/",
        pageScore.getUrl());
    Assert.assertEquals(4, pageScore.getScore().intValue());
  }

  @AfterClass
  public static void destroy() throws IOException {
    devServer.stop();
    FileUtils.deleteDirectory(tempPath.toFile());
  }
}
