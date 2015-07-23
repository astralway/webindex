/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.commoncrawl.data.inbound;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import com.google.common.base.Joiner;
import com.google.common.net.HostSpecifier;
import com.google.common.net.InternetDomainName;
import org.apache.commons.lang.ArrayUtils;

public class Link {

  private URL url;
  private String anchorText;

  private Link(URL url, String anchorText) {
    this.url = url;
    this.anchorText = anchorText;
  }

  public static Link from(String url, String anchorText) throws ParseException {
    try {
      URL u = new URL(url);
      if (!HostSpecifier.isValid(u.getHost())) {
        throw new ParseException("Invalid host: " + u.getHost(), 0);
      }
      if (InternetDomainName.isValid(u.getHost())) {
        if (!InternetDomainName.from(u.getHost()).isUnderPublicSuffix()) {
          throw new ParseException("Bad domain: " + u.getHost(), 0);
        }
      }
      return new Link(u, anchorText);
    } catch (MalformedURLException e) {
      throw new ParseException(e.getMessage(), 0);
    }
  }

  public static Link from(String url) throws ParseException {
    return from(url, "");
  }

  public static Link fromValid(String url, String anchorText) {
    try {
      return from(url, anchorText);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  public static Link fromValid(String url) {
    return fromValid(url, "");
  }

  public static boolean isValid(String url) {
    try {
      from(url);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public String getHost() {
    return url.getHost();
  }

  private static String reverseDomain(String domain) {
    String[] domainArgs = domain.split("\\.");
    ArrayUtils.reverse(domainArgs);
    return Joiner.on(".").join(domainArgs);
  }

  public String getReverseHost() {
    if (hasDomain()) {
      return reverseDomain(getHost());
    } else {
      return getHost();
    }
  }

  public String getAnchorText() {
    return anchorText;
  }

  public boolean hasDomain() {
    return InternetDomainName.isValid(getHost());
  }

  public String getTopPrivate() {
    if (hasDomain()) {
      return InternetDomainName.from(getHost()).topPrivateDomain().name();
    }
    return getHost();
  }

  public String getReverseTopPrivate() {
    if (hasDomain()) {
      return reverseDomain(getTopPrivate());
    }
    return getTopPrivate();
  }

  public String getUrl() {
    return url.toString();
  }

  public String getUri() {
    StringBuilder uri = new StringBuilder();
    uri.append(getReverseHost());
    if ((url.getPort() != -1) && (url.getPort() != 80)) {
      uri.append(":" + Integer.toString(url.getPort()));
    }
    uri.append(url.getPath());
    if (url.getQuery() != null && !url.getQuery().isEmpty()) {
      uri.append("?" + url.getQuery());
    }
    return uri.toString();
  }

  public boolean isImage() {
    return url.getFile().matches("([^\\s]+(\\.(?i)(jpeg|jpg|png|gif|bmp))$)");
  }
}
