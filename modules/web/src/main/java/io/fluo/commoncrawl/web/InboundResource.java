package io.fluo.commoncrawl.web;

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
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.core.DataUtil;
import io.fluo.commoncrawl.core.models.DomainStats;
import io.fluo.commoncrawl.core.models.Links;
import io.fluo.commoncrawl.core.models.Page;
import io.fluo.commoncrawl.core.models.Pages;
import io.fluo.commoncrawl.web.util.Pager;
import io.fluo.commoncrawl.web.util.WebUtil;
import io.fluo.commoncrawl.web.views.HomeView;
import io.fluo.commoncrawl.web.views.LinksView;
import io.fluo.commoncrawl.web.views.PageView;
import io.fluo.commoncrawl.web.views.PagesView;
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
public class InboundResource {

  private static final Logger log = LoggerFactory.getLogger(InboundResource.class);
  private static final int PAGE_SIZE = 25;

  private FluoConfiguration fluoConfig;
  private DataConfig dataConfig;
  private Connector conn;
  private Gson gson = new Gson();

  public InboundResource(FluoConfiguration fluoConfig, Connector conn, DataConfig dataConfig) {
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
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      new Pager(scanner, "d:" + DataUtil.reverseDomain(domain), ColumnConstants.RANK, next, pageNum,
                PAGE_SIZE) {

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
      }.getPage();
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
    Long score = (long) 0;
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + DataUtil.toUri(url), ColumnConstants.PAGE));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch (entry.getKey().getColumnQualifier().toString()) {
          case ColumnConstants.INCOUNT:
            incount = getLongValue(entry);
            break;
          case ColumnConstants.SCORE:
            score = getLongValue(entry);
            break;
          case ColumnConstants.CUR:
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
    page.setScore(score);
    page.setDomain(WebUtil.getDomain(page.getUrl()));
    return page;
  }

  private DomainStats getDomainStats(String domain) {
    DomainStats stats = new DomainStats(domain);
    Scanner scanner = null;
    try {
      scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("d:" + DataUtil.reverseDomain(domain), ColumnConstants.DOMAIN));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch (entry.getKey().getColumnQualifier().toString()) {
          case ColumnConstants.PAGECOUNT:
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
  public LinksView getLinks(@NotNull @QueryParam("pageUrl") String pageUrl,
                            @NotNull @QueryParam("linkType") String linkType,
                            @DefaultValue("") @QueryParam("next") String next,
                            @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    Links links = new Links(pageUrl, linkType, pageNum);
    log.info("links url {}", links.getUrl());

    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String row = "p:" + DataUtil.toUri(pageUrl);

      if (linkType.equals("in")) {
        Page page = getPage(pageUrl);
        String cf = ColumnConstants.INLINKS;
        links.setTotal(page.getNumInbound());
        new Pager(scanner, "p:" + DataUtil.toUri(pageUrl), cf, next, pageNum, PAGE_SIZE) {

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
        }.getPage();
      } else {
        scanner.setRange(Range.exact(row, ColumnConstants.PAGE, ColumnConstants.CUR));
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
      log.error("Failed to parse URL {}", pageUrl);
    }
    return new LinksView(links);
  }
}
