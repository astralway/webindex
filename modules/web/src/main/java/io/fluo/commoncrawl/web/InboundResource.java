package io.fluo.commoncrawl.web;

import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.core.DataUtil;
import io.fluo.commoncrawl.web.models.DomainStats;
import io.fluo.commoncrawl.web.models.PageStats;
import io.fluo.commoncrawl.web.models.WebLink;
import io.fluo.commoncrawl.web.models.Links;
import io.fluo.commoncrawl.web.models.PageScore;
import io.fluo.commoncrawl.web.models.Pages;
import io.fluo.commoncrawl.web.util.Pager;
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

  private FluoConfiguration fluoConfig;
  private DataConfig dataConfig;
  private Connector conn;

  public InboundResource(FluoConfiguration fluoConfig, Connector conn, DataConfig dataConfig) {
    this.fluoConfig = fluoConfig;
    this.conn = conn;
    this.dataConfig = dataConfig;
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public HomeView getHome() {
    return new HomeView();
  }

  @GET
  @Path("pages")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PagesView getPages(@QueryParam("domain") String domain,
                          @DefaultValue("") @QueryParam("next") String next,
                          @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {
    DomainStats stats = getDomainStats(domain);
    Pages pages = new Pages(domain, pageNum);
    pages.setTotal(stats.getTotal());
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      new Pager(scanner, "d:" + DataUtil.reverseDomain(domain), ColumnConstants.RANK, next, pageNum) {

        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String url = DataUtil.toUrl(entry.getKey().getColumnQualifier().toString().split(":", 2)[1]);
          Long count = Long.parseLong(entry.getValue().toString());
          pages.addPage(new PageScore(url, count));
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
  public PageView getPage(@QueryParam("url") String url) {
    return new PageView(getPageStats(url));
  }

  private PageStats getPageStats(String url) {
    PageStats page = new PageStats(url);
    Scanner scanner = null;
    try {
      scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + DataUtil.toUri(url), ColumnConstants.PAGE));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch(entry.getKey().getColumnQualifier().toString()) {
          case ColumnConstants.INCOUNT:
            page.setNumInbound(getIntValue(entry));
            break;
          case ColumnConstants.OUTCOUNT:
            page.setNumOutbound(getIntValue(entry));
            break;
          case ColumnConstants.SCORE:
            page.setScore(getIntValue(entry));
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
        switch(entry.getKey().getColumnQualifier().toString()) {
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
  private static Integer getIntValue(Map.Entry<Key, Value> entry) {
    return Integer.parseInt(entry.getValue().toString());
  }
  private static Long getLongValue(Map.Entry<Key, Value> entry) {
    return Long.parseLong(entry.getValue().toString());
  }
  @GET
  @Path("links")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public LinksView getLinks(@QueryParam("pageUrl") String pageUrl,
                          @QueryParam("linkType") String linkType,
                          @DefaultValue("") @QueryParam("next") String next,
                          @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {

    PageStats stats = getPageStats(pageUrl);
    Links links = new Links(pageUrl, linkType, pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String cf = ColumnConstants.INLINKS;
      if (linkType.equals("out")) {
        cf = ColumnConstants.OUTLINKS;
        links.setTotal(stats.getNumOutbound());
      } else {
        links.setTotal(stats.getNumInbound());
      }
      new Pager(scanner, "p:" + DataUtil.toUri(pageUrl), cf, next, pageNum) {

        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String url = DataUtil.toUrl(entry.getKey().getColumnQualifier().toString());
          String anchorText = entry.getValue().toString();
          links.addLink(new WebLink(url, anchorText));
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          links.setNext(entry.getKey().getColumnQualifier().toString());
        }
      }.getPage();
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    } catch (MalformedURLException e) {
      log.error("Failed to parse URL {}", pageUrl);
    }
    return new LinksView(links);
  }
}
