package io.fluo.commoncrawl.web;

import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Joiner;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.web.models.Link;
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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
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

  private static String reverseDomain(String domain) {
    String[] domainArgs = domain.split("\\.");
    ArrayUtils.reverse(domainArgs);
    return Joiner.on(".").join(domainArgs);
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public HomeView getHome() {
    return new HomeView();
  }

  @GET
  @Path("site")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public SiteView getSite(@QueryParam("domain") String domain) {
    Site tp = new Site(domain);
    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("d:" + reverseDomain(domain)));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      long num = 0;
      while (iterator.hasNext() && (num <= 50)) {
        Map.Entry<Key, Value> entry = iterator.next();
        Key key = entry.getKey();
        Value value = entry.getValue();
        String[] colArgs = key.getColumnFamily().toString().split("\t", 2);
        if (colArgs.length == 2) {
          tp.addPage(new PageCount(colArgs[1], Long.parseLong(value.toString())));
          num++;
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new SiteView(tp);
  }

  @GET
  @Path("page")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
  public PageView getPage(@QueryParam("url") String url) {
    Page page = new Page(url);

    try {
      Scanner scanner = conn.createScanner(dataConfig.accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact(url));
      Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
      long num = 0;
      while (iterator.hasNext() && (num <= 50)) {
        Map.Entry<Key, Value> entry = iterator.next();
        Key key = entry.getKey();
        Value value = entry.getValue();
        if (key.getColumnFamily().toString().startsWith("p:")) {
          String[] colArgs = key.getColumnFamily().toString().split("\t", 2);
          if (colArgs.length == 2) {
            page.addLink(new Link(colArgs[0], colArgs[1]));
            num++;
          }
        }
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", dataConfig.accumuloIndexTable);
    }
    return new PageView(page);
  }
}
