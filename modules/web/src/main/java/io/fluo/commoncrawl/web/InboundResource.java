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
import io.fluo.commoncrawl.web.models.WebLink;
import io.fluo.commoncrawl.web.models.Page;
import io.fluo.commoncrawl.web.models.PageCount;
import io.fluo.commoncrawl.web.models.Site;
import io.fluo.commoncrawl.web.views.HomeView;
import io.fluo.commoncrawl.web.views.PageView;
import io.fluo.commoncrawl.web.views.SiteView;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
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
  @Path("site")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public SiteView getSite(@QueryParam("domain") String domain,
                          @DefaultValue("") @QueryParam("next") String next,
                          @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {
    Site site = new Site(domain, pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);

      String row = "d:" + DataUtil.reverseDomain(domain);
      if (next.isEmpty()) {
        scanner.setRange(Range.exact(row, AccumuloConstants.PAGEDESC));
      } else {
        scanner.setRange(new Range(new Key(row, AccumuloConstants.PAGEDESC, next),
                                   new Key(row, AccumuloConstants.PAGEDESC).followingKey(PartialKey.ROW)));
      }
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      if (next.isEmpty() && (pageNum > 0)) {
        long skip = 0;
        while (skip < (pageNum*25)) {
          iterator.next();
          skip++;
        }
      }
      long num = 0;
      while (iterator.hasNext() && (num < 26)) {
        Map.Entry<Key, Value> entry = iterator.next();
        Key key = entry.getKey();
        Value value = entry.getValue();
        String[] cqArgs = key.getColumnQualifier().toString().split(":", 2);
        if (cqArgs.length == 2) {
          if (num == 25) {
            site.setNext(key.getColumnQualifier().toString());
          } else {
            site.addPage(new PageCount(DataUtil.toUrl(cqArgs[1]), Long.parseLong(value.toString())));
          }
          num++;
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new SiteView(site);
  }

  @GET
  @Path("page")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PageView getPage(@QueryParam("url") String url,
                          @DefaultValue("") @QueryParam("domain") String domain,
                          @DefaultValue("") @QueryParam("next") String next,
                          @DefaultValue("0") @QueryParam("pageNum") Integer pageNum) {
    Page page = new Page(url, domain, pageNum);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      String row = "p:" + DataUtil.toUri(url);
      if (next.isEmpty()) {
        scanner.setRange(Range.exact(row, AccumuloConstants.INLINKS));
      } else {
        scanner.setRange(new Range(new Key(row, AccumuloConstants.INLINKS, next),
                                   new Key(row, AccumuloConstants.INLINKS).followingKey(PartialKey.ROW)));
      }
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      if (next.isEmpty() && (pageNum > 0)) {
        long skip = 0;
        while (skip < (pageNum*25)) {
          Map.Entry<Key, Value> entry = iterator.next();
          skip++;
        }
      }
      long num = 0;
      while (iterator.hasNext() && (num < 26)) {
        Map.Entry<Key, Value> entry = iterator.next();
        Key key = entry.getKey();
        String link = key.getColumnQualifier().toString();
        String anchorText = entry.getValue().toString();
        if (num == 25) {
          page.setNext(link);
        } else {
          page.addLink(new WebLink(DataUtil.toUrl(link), anchorText));
        }
        num++;
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    } catch (MalformedURLException e) {
      log.error("Failed to parse URL {}", url);
    }
    return new PageView(page);
  }
}
