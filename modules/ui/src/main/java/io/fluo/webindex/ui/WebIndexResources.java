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

import java.net.MalformedURLException;
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
import io.fluo.api.config.FluoConfiguration;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.core.DataUtil;
import io.fluo.webindex.core.models.DomainStats;
import io.fluo.webindex.core.models.Links;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.core.models.Pages;
import io.fluo.webindex.core.models.TopResults;
import io.fluo.webindex.ui.util.Pager;
import io.fluo.webindex.ui.util.WebUtil;
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

  private FluoConfiguration fluoConfig;
  private DataConfig dataConfig;
  private Connector conn;
  private Gson gson = new Gson();

  public WebIndexResources(FluoConfiguration fluoConfig, Connector conn, DataConfig dataConfig) {
    this.fluoConfig = fluoConfig;
    this.conn = conn;
    this.dataConfig = dataConfig;
  }

  private static Integer getIntValue(Map.Entry<Key, Value> entry) {
    return Integer.parseInt(entry.getValue().toString());
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
    String row = "d:" + DataUtil.reverseDomain(domain);
    String cf = Constants.RANK;
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      Pager pager = new Pager(scanner, Range.exact(row, cf), PAGE_SIZE) {
        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String url =
              DataUtil.toUrl(entry.getKey().getColumnQualifier().toString().split(":", 2)[1]);
          Long count = Long.parseLong(entry.getValue().toString());
          pages.addPage(url, count);
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          pages.setNext(entry.getKey().getColumnQualifier().toString());
        }
      };
      if (next.isEmpty()) {
        pager.getPage(pageNum);
      } else {
        pager.getPage(new Key(row, cf, next));
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

  private Page getPage(String url) {
    Page page = null;
    Long incount = (long) 0;
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + DataUtil.toUri(url), Constants.PAGE));
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
    } catch (MalformedURLException e) {
      log.error("Failed to parse URL {}", url);
    }
    if (page == null) {
      page = new Page(url);
    }
    page.setNumInbound(incount);
    page.setDomain(WebUtil.getDomain(page.getUrl()));
    return page;
  }

  private DomainStats getDomainStats(String domain) {
    DomainStats stats = new DomainStats(domain);
    Scanner scanner = null;
    try {
      scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("d:" + DataUtil.reverseDomain(domain), Constants.DOMAIN));
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
  public LinksView getLinks(@NotNull @QueryParam("url") String url,
      @NotNull @QueryParam("linkType") String linkType,
      @DefaultValue("") @QueryParam("next") String next,
      @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    Links links = new Links(url, linkType, pageNum);
    log.info("links url {}", links.getUrl());

    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String row = "p:" + DataUtil.toUri(url);
      if (linkType.equals("in")) {
        Page page = getPage(url);
        String cf = Constants.INLINKS;
        links.setTotal(page.getNumInbound());
        Pager pager = new Pager(scanner, Range.exact(row, cf), PAGE_SIZE) {

          @Override
          public void foundPageEntry(Map.Entry<Key, Value> entry) {
            String url = DataUtil.toUrl(entry.getKey().getColumnQualifier().toString());
            String anchorText = entry.getValue().toString();
            links.addLink(url, anchorText);
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
          for (Page.Link l : curPage.getOutboundLinks()) {
            if (skip < (pageNum * PAGE_SIZE)) {
              skip++;
            } else if (add < PAGE_SIZE) {
              links.addLink(l.getUrl(), l.getAnchorText());
              add++;
            } else {
              links.setNext(l.getUrl());
              break;
            }
          }
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    } catch (MalformedURLException e) {
      log.error("Failed to parse URL {}", url);
    }
    return new LinksView(links);
  }

  @GET
  @Path("top")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public TopView getTop(@NotNull @QueryParam("resultType") String resultType,
      @DefaultValue("") @QueryParam("next") String next,
      @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    TopResults results = new TopResults();
    if (resultType.equals(Constants.INCOUNT)) {
      results.setResultType(resultType);
      results.setPageNum(pageNum);
      try {
        Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
        String row = "t:" + resultType;
        String cf = Constants.RANK;
        Pager pager = new Pager(scanner, Range.exact(row, cf), PAGE_SIZE) {

          @Override
          public void foundPageEntry(Map.Entry<Key, Value> entry) {
            String url =
                DataUtil.toUrl(entry.getKey().getColumnQualifier().toString().split(":", 2)[1]);
            Long num = Long.parseLong(entry.getValue().toString());
            results.addResult(url, num);
          }

          @Override
          public void foundNextEntry(Map.Entry<Key, Value> entry) {
            results.setNext(entry.getKey().getColumnQualifier().toString());
          }
        };
        if (next.isEmpty()) {
          pager.getPage(pageNum);
        } else {
          pager.getPage(new Key(row, cf, next));
        }
      } catch (TableNotFoundException e) {
        log.error("Table {} not found", dataConfig.accumuloIndexTable);
      }
    }

    return new TopView(results);
  }
}
