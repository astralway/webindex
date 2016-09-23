/*
 * Copyright 2016 Webindex authors (see AUTHORS)
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

public class Link implements Serializable, Comparable<Link> {

  private static final long serialVersionUID = 1L;

  private String url;
  private String uri;
  private String anchorText;

  public Link() {}

  public Link(String uri, String anchorText) {
    Objects.requireNonNull(uri);
    Objects.requireNonNull(anchorText);
    this.url = URL.fromUri(uri).toString();
    this.uri = uri;
    this.anchorText = anchorText;
  }

  public String getUrl() {
    return url;
  }

  public String getUri() {
    return uri;
  }

  public String getAnchorText() {
    return anchorText;
  }


  public static Link of(String uri, String anchorText) {
    return new Link(uri, anchorText);
  }

  public static Link of(String uri) {
    return new Link(uri, "");
  }

  public static Link of(URL url, String anchorText) {
    return new Link(url.toUri(), anchorText);
  }

  public static Link of(URL url) {
    return new Link(url.toUri(), "");
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Link) {
      Link other = (Link) o;
      return url.equals(other.url) && uri.equals(other.uri);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = url.hashCode();
    result = 31 * result + uri.hashCode();
    return result;
  }

  @Override
  public int compareTo(Link o) {
    int c = uri.compareTo(o.uri);
    if (c == 0) {
      c = url.compareTo(o.url);
    }

    return c;
  }
}
