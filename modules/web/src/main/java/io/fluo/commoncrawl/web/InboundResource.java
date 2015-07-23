package io.fluo.commoncrawl.web;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Joiner;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.commoncrawl.web.models.PageCount;
import io.fluo.commoncrawl.web.models.TopPages;
import org.apache.commons.lang.ArrayUtils;

@Path("/top-pages")
@Produces(MediaType.APPLICATION_JSON)
public class InboundResource {
  private FluoConfiguration config;

  public InboundResource(FluoConfiguration config) {
    this.config = config;
  }

  private static String reverseDomain(String domain) {
    String[] domainArgs = domain.split("\\.");
    ArrayUtils.reverse(domainArgs);
    return Joiner.on(".").join(domainArgs);
  }

  @GET
  public TopPages getTopPages(@QueryParam("domain") String domain) {
    TopPages tp = new TopPages(domain);
    try (FluoClient client = FluoFactory.newClient(config);
         Snapshot snapshot = client.newSnapshot()) {
      ScannerConfiguration sconfig = new ScannerConfiguration();
      sconfig.setSpan(Span.exact("d:" + reverseDomain(domain)));
      RowIterator rowIter = snapshot.get(sconfig);
      if (rowIter.hasNext()) {
        ColumnIterator colIter = rowIter.next().getValue();
        long num = 0;
        while (colIter.hasNext() && (num <= 50)) {
          Map.Entry<Column, Bytes> entry = colIter.next();
          Column col = entry.getKey();
          Bytes val = entry.getValue();
          String[] colArgs = col.getFamily().toString().split("\t", 2);
          if (colArgs.length == 2) {
            tp.addPage(new PageCount(colArgs[1], Long.parseLong(val.toString())));
            num++;
          }
        }
      }
    }
    return tp;
  }
}
