/*
 * Copyright 2016 Fluo authors (see AUTHORS)
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

package io.fluo.webindex.core.models;

import java.io.Serializable;
import java.util.Objects;

public class Link implements Serializable, Comparable<Link> {

  private static final long serialVersionUID = 1L;

  private String url;
  private String pageID;
  private String anchorText;

  public Link() {}

  public Link(String pageID, String anchorText) {
    Objects.requireNonNull(pageID);
    Objects.requireNonNull(anchorText);
    this.url = URL.fromPageID(pageID).toString();
    this.pageID = pageID;
    this.anchorText = anchorText;
  }

  public String getUrl() {
    return url;
  }

  public String getPageID() {
    return pageID;
  }

  public String getAnchorText() {
    return anchorText;
  }


  public static Link of(String pageID, String anchorText) {
    return new Link(pageID, anchorText);
  }

  public static Link of(String pageID) {
    return new Link(pageID, "");
  }

  public static Link of(URL url, String anchorText) {
    return new Link(url.toPageID(), anchorText);
  }

  public static Link of(URL url) {
    return new Link(url.toPageID(), "");
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Link) {
      Link other = (Link) o;
      return url.equals(other.url) && pageID.equals(other.pageID);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = url.hashCode();
    result = 31 * result + pageID.hashCode();
    return result;
  }

  @Override
  public int compareTo(Link o) {
    int c = pageID.compareTo(o.pageID);
    if (c == 0) {
      c = url.compareTo(o.url);
    }

    return c;
  }
}
