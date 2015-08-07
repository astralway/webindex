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
import io.fluo.commoncrawl.core.AccumuloConstants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.core.DataUtil;
import io.fluo.commoncrawl.web.models.Page;
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
    Pages site = new Pages(domain, pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      new Pager(scanner, "d:" + DataUtil.reverseDomain(domain), AccumuloConstants.PAGEDESC, next, pageNum) {

        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String url = DataUtil.toUrl(entry.getKey().getColumnQualifier().toString().split(":", 2)[1]);
          Long count = Long.parseLong(entry.getValue().toString());
          site.addPage(new PageScore(url, count));
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          site.setNext(entry.getKey().getColumnQualifier().toString());
        }
      }.getPage();
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new PagesView(site);
  }

  @GET
  @Path("page")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PageView getPage(@QueryParam("url") String url) {
    Page page = new Page(url);
    Scanner scanner = null;
    try {
      scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + DataUtil.toUri(url), AccumuloConstants.STATS));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, Value> entry = iterator.next();
        switch(entry.getKey().getColumnQualifier().toString()) {
          case AccumuloConstants.INLINKCOUNT:
            page.setNumInbound(getIntValue(entry));
            break;
          case AccumuloConstants.OUTLINKCOUNT:
            page.setNumOutbound(getIntValue(entry));
            break;
          case AccumuloConstants.PAGESCORE:
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
    return new PageView(page);
  }

  private static Integer getIntValue(Map.Entry<Key, Value> entry) {
    return Integer.parseInt(entry.getValue().toString());
  }

  @GET
  @Path("links")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public LinksView getLinks(@QueryParam("pageUrl") String pageUrl,
                          @QueryParam("linkType") String linkType,
                          @DefaultValue("") @QueryParam("next") String next,
                          @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {
    Links page = new Links(pageUrl, linkType, pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String cf = AccumuloConstants.INLINKS;
      if (linkType.equals("out")) {
        cf = AccumuloConstants.OUTLINKS;
      }
      new Pager(scanner, "p:" + DataUtil.toUri(pageUrl), cf, next, pageNum) {

        @Override
        public void foundPageEntry(Map.Entry<Key, Value> entry) {
          String url = DataUtil.toUrl(entry.getKey().getColumnQualifier().toString());
          String anchorText = entry.getValue().toString();
          page.addLink(new WebLink(url, anchorText));
        }

        @Override
        public void foundNextEntry(Map.Entry<Key, Value> entry) {
          page.setNext(entry.getKey().getColumnQualifier().toString());
        }
      }.getPage();
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    } catch (MalformedURLException e) {
      log.error("Failed to parse URL {}", pageUrl);
    }
    return new LinksView(page);
  }
}
