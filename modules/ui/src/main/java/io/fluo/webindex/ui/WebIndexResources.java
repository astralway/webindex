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

package io.fluo.webindex.ui;

import java.util.Iterator;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.models.DomainStats;
import io.fluo.webindex.core.models.Link;
import io.fluo.webindex.core.models.Links;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.core.models.Pages;
import io.fluo.webindex.core.models.TopResults;
import io.fluo.webindex.core.models.URL;
import io.fluo.webindex.ui.util.Pager;
import io.fluo.webindex.ui.util.WebUrl;
import io.fluo.webindex.ui.views.HomeView;
import io.fluo.webindex.ui.views.LinksView;
import io.fluo.webindex.ui.views.PageView;
import io.fluo.webindex.ui.views.PagesView;
import io.fluo.webindex.ui.views.TopView;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class WebIndexResources {

  private static final Logger log = LoggerFactory.getLogger(WebIndexResources.class);
  private static final int PAGE_SIZE = 25;

  private DataConfig dataConfig;
  private Connector conn;
  private Gson gson = new Gson();

  public WebIndexResources(Connector conn, DataConfig dataConfig) {
    this.conn = conn;
    this.dataConfig = dataConfig;
  }

  private static Long getLongValue(Map.Entry<Key, Value> entry) {
    return Long.parseLong(entry.getValue().toString());
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public HomeView getHome() {
    return new HomeView();
  }

  @GET
  @Path("pages")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PagesView getPages(@NotNull @QueryParam("domain") String domain,
      @DefaultValue("") @QueryParam("next") String next,
      @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {
    DomainStats stats = getDomainStats(domain);
    Pages pages = new Pages(domain, pageNum);
    log.info("Setting total to {}", stats.getTotal());
    pages.setTotal(stats.getTotal());
    String row = "d:" + URL.reverseHost(domain);
    String cf = Constants.RANK;
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      Pager pager = new Pager(scanner, Range.prefix(row + ":"), PAGE_SIZE) {
        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {

          String url =
              URL.fromPageID(entry.getKey().getRowData().toString().split(":", 4)[3]).toString();
          Long count = Long.parseLong(entry.getValue().toString());
          pages.addPage(url, count);
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          pages.setNext(entry.getKey().getRowData().toString().split(":", 3)[2]);
        }
      };
      if (next.isEmpty()) {
        pager.getPage(pageNum);
      } else {
        pager.getPage(new Key(row + ":" + next, cf, ""));

      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new PagesView(pages);
  }

  @GET
  @Path("page")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PageView getPageView(@NotNull @QueryParam("url") String url) {
    return new PageView(getPage(url));
  }

  private Page getPage(String rawUrl) {
    Page page = null;
    Long incount = (long) 0;
    URL url;
    try {
      url = WebUrl.from(rawUrl);
    } catch (Exception e) {
      log.error("Failed to parse URL {}", rawUrl);
      return null;
    }

    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + url.toPageID(), Constants.PAGE));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch (entry.getKey().getColumnQualifier().toString()) {
          case Constants.INCOUNT:
            incount = getLongValue(entry);
            break;
          case Constants.CUR:
            page = gson.fromJson(entry.getValue().toString(), Page.class);
            break;
          default:
            log.error("Unknown page stat {}", entry.getKey().getColumnQualifier());
        }
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }

    if (page == null) {
      page = new Page(url.toPageID());
    }
    page.setNumInbound(incount);
    return page;
  }

  private DomainStats getDomainStats(String domain) {
    DomainStats stats = new DomainStats(domain);
    Scanner scanner;
    try {
      scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("d:" + URL.reverseHost(domain), Constants.DOMAIN));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch (entry.getKey().getColumnQualifier().toString()) {
          case Constants.PAGECOUNT:
            stats.setTotal(getLongValue(entry));
            break;
          default:
            log.error("Unknown page domain {}", entry.getKey().getColumnQualifier());
        }
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
    return stats;
  }

  @GET
  @Path("links")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public LinksView getLinks(@NotNull @QueryParam("url") String rawUrl,
      @NotNull @QueryParam("linkType") String linkType,
      @DefaultValue("") @QueryParam("next") String next,
      @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    Links links = new Links(rawUrl, linkType, pageNum);

    URL url;
    try {
      url = WebUrl.from(rawUrl);
    } catch (Exception e) {
      log.error("Failed to parse URL: " + rawUrl);
      return new LinksView(links);
    }

    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String row = "p:" + url.toPageID();
      if (linkType.equals("in")) {
        Page page = getPage(rawUrl);
        String cf = Constants.INLINKS;
        links.setTotal(page.getNumInbound());
        Pager pager = new Pager(scanner, Range.exact(row, cf), PAGE_SIZE) {

          @Override
          public void foundPageEntry(Map.Entry<Key, Value> entry) {
            String pageID = entry.getKey().getColumnQualifier().toString();
            String anchorText = entry.getValue().toString();
            links.addLink(Link.of(pageID, anchorText));
          }

          @Override
          public void foundNextEntry(Map.Entry<Key, Value> entry) {
            links.setNext(entry.getKey().getColumnQualifier().toString());
          }
        };
        if (next.isEmpty()) {
          pager.getPage(pageNum);
        } else {
          pager.getPage(new Key(row, cf, next));
        }
      } else {
        scanner.setRange(Range.exact(row, Constants.PAGE, Constants.CUR));
        Iterator<Map.Entry<Key, Value>> iter = scanner.iterator();
        if (iter.hasNext()) {
          Page curPage = gson.fromJson(iter.next().getValue().toString(), Page.class);
          links.setTotal(curPage.getNumOutbound());
          int skip = 0;
          int add = 0;
          for (Link l : curPage.getOutboundLinks()) {
            if (skip < (pageNum * PAGE_SIZE)) {
              skip++;
            } else if (add < PAGE_SIZE) {
              links.addLink(l);
              add++;
            } else {
              links.setNext(l.getPageID());
              break;
            }
          }
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new LinksView(links);
  }

  @GET
  @Path("top")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public TopView getTop(@DefaultValue("") @QueryParam("next") String next,
      @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    TopResults results = new TopResults();

    results.setPageNum(pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      Pager pager = new Pager(scanner, Range.prefix("t:"), PAGE_SIZE) {

        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String row = entry.getKey().getRow().toString();
          String url = URL.fromPageID(row.split(":", 3)[2]).toString();
          Long num = Long.parseLong(entry.getValue().toString());
          results.addResult(url, num);
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          results.setNext(entry.getKey().getRow().toString());
        }
      };
      if (next.isEmpty()) {
        pager.getPage(pageNum);
      } else {
        pager.getPage(new Key(next));
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new TopView(results);
  }
}
