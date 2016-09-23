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

package webindex.core.models;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.Gson;

public class Page implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final Page EMPTY = new Page();
  public static final Page DELETE = new Page(true);
  public static final String DELETE_JSON = "delete";

  private String url;
  private String uri;
  private Long numInbound;
  private Long numOutbound = 0L;
  private String crawlDate;
  private String server;
  private String title;
  // This is a tree set so that json serializes consistently. Wanted to use hashset and sort on
  // serialization, but could not figure out how to do that.
  private Set<Link> outboundLinks = new TreeSet<>();
  private transient boolean isDelete = false;

  private Page() {}

  private Page(boolean isDelete) {
    this.isDelete = isDelete;
  }

  public Page(String uri) {
    Objects.requireNonNull(uri);
    this.url = URL.fromUri(uri).toString();
    this.uri = uri;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getUrl() {
    return url;
  }

  public String getUri() {
    return uri;
  }

  public Set<Link> getOutboundLinks() {
    return outboundLinks;
  }

  /**
   * @return True if page did not already contain link
   */
  public boolean addOutbound(Link link) {
    boolean added = outboundLinks.add(link);
    if (added) {
      numOutbound++;
    }
    return added;
  }

  /**
   * @return True if link was removed
   */
  public boolean removeOutbound(Link link) {
    boolean removed = outboundLinks.remove(link);
    if (removed) {
      numOutbound--;
    }
    return removed;
  }

  public boolean isEmpty() {
    return url == null && outboundLinks.isEmpty();
  }

  public String getDomain() {
    return URL.fromUri(uri).getDomain();
  }

  public Long getNumInbound() {
    return numInbound;
  }

  public void setNumInbound(Long numInbound) {
    this.numInbound = numInbound;
  }

  public Long getNumOutbound() {
    return numOutbound;
  }

  public String getCrawlDate() {
    return crawlDate;
  }

  public void setCrawlDate(String crawlDate) {
    this.crawlDate = crawlDate;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public static Page fromJson(Gson gson, String pageJson) {
    if (pageJson.isEmpty()) {
      return Page.EMPTY;
    }

    if (pageJson.equals(DELETE_JSON)) {
      return Page.DELETE;
    }

    return gson.fromJson(pageJson, Page.class);
  }
}
