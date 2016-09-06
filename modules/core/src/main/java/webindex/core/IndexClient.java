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

package webindex.core;

import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.models.DomainStats;
import webindex.core.models.Link;
import webindex.core.models.Links;
import webindex.core.models.Page;
import webindex.core.models.Pages;
import webindex.core.models.TopResults;
import webindex.core.models.URL;
import webindex.core.util.Pager;

public class IndexClient {

  private static final Logger log = LoggerFactory.getLogger(IndexClient.class);
  private static final int PAGE_SIZE = 25;

  private Connector conn;
  private String accumuloIndexTable;
  private Gson gson = new Gson();

  public IndexClient(String accumuloIndexTable, Connector conn) {
    this.accumuloIndexTable = accumuloIndexTable;
    this.conn = conn;
  }

  public TopResults getTopResults(String next, int pageNum) {

    TopResults results = new TopResults();

    results.setPageNum(pageNum);
    try {
      Scanner scanner = conn.createScanner(accumuloIndexTable, Authorizations.EMPTY);
      Pager pager = Pager.build(scanner, Range.prefix("t:"), PAGE_SIZE, entry -> {
        String row = entry.getKey().getRow().toString();
        if (entry.isNext()) {
          results.setNext(row);
        } else {
          String url = URL.fromPageID(row.split(":", 3)[2]).toString();
          Long num = Long.parseLong(entry.getValue().toString());
          results.addResult(url, num);
        }
      });
      if (next.isEmpty()) {
        pager.read(pageNum);
      } else {
        pager.read(new Key(next));
      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", accumuloIndexTable);
    }
    return results;
  }

  private static Long getLongValue(Map.Entry<Key, Value> entry) {
    return Long.parseLong(entry.getValue().toString());
  }

  public Page getPage(String rawUrl) {
    Page page = null;
    Long incount = (long) 0;
    URL url;
    try {
      url = URL.from(rawUrl);
    } catch (Exception e) {
      log.error("Failed to parse URL {}", rawUrl);
      return null;
    }

    try {
      Scanner scanner = conn.createScanner(accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("p:" + url.toPageID(), Constants.PAGE));
      for (Map.Entry<Key, Value> entry : scanner) {
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

  public DomainStats getDomainStats(String domain) {
    DomainStats stats = new DomainStats(domain);
    Scanner scanner;
    try {
      scanner = conn.createScanner(accumuloIndexTable, Authorizations.EMPTY);
      scanner.setRange(Range.exact("d:" + URL.reverseHost(domain), Constants.DOMAIN));
      for (Map.Entry<Key, Value> entry : scanner) {
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

  public Pages getPages(String domain, String next, int pageNum) {
    DomainStats stats = getDomainStats(domain);
    Pages pages = new Pages(domain, pageNum);
    pages.setTotal(stats.getTotal());
    String row = "d:" + URL.reverseHost(domain);
    String cf = Constants.RANK;
    try {
      Scanner scanner = conn.createScanner(accumuloIndexTable, Authorizations.EMPTY);
      Pager pager =
          Pager.build(scanner, Range.prefix(row + ":"), PAGE_SIZE, entry -> {
            if (entry.isNext()) {
              pages.setNext(entry.getKey().getRowData().toString().split(":", 3)[2]);
            } else {
              String url =
                  URL.fromPageID(entry.getKey().getRowData().toString().split(":", 4)[3])
                      .toString();
              Long count = Long.parseLong(entry.getValue().toString());
              pages.addPage(url, count);
            }
          });
      if (next.isEmpty()) {
        pager.read(pageNum);
      } else {
        pager.read(new Key(row + ":" + next, cf, ""));

      }
    } catch (TableNotFoundException e) {
      log.error("Table {} not found", accumuloIndexTable);
    }
    return pages;
  }

  public Links getLinks(String rawUrl, String linkType, String next, int pageNum) {

    Links links = new Links(rawUrl, linkType, pageNum);

    URL url;
    try {
      url = URL.from(rawUrl);
    } catch (Exception e) {
      log.error("Failed to parse URL: " + rawUrl);
      return links;
    }

    try {
      Scanner scanner = conn.createScanner(accumuloIndexTable, Authorizations.EMPTY);
      String row = "p:" + url.toPageID();
      if (linkType.equals("in")) {
        Page page = getPage(rawUrl);
        String cf = Constants.INLINKS;
        links.setTotal(page.getNumInbound());
        Pager pager = Pager.build(scanner, Range.exact(row, cf), PAGE_SIZE, entry -> {
          String pageID = entry.getKey().getColumnQualifier().toString();
          if (entry.isNext()) {
            links.setNext(pageID);
          } else {
            String anchorText = entry.getValue().toString();
            links.addLink(Link.of(pageID, anchorText));
          }
        });
        if (next.isEmpty()) {
          pager.read(pageNum);
        } else {
          pager.read(new Key(row, cf, next));
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
      log.error("Table {} not found", accumuloIndexTable);
    }
    return links;
  }
}
