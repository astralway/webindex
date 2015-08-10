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

package io.fluo.commoncrawl.data.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import com.google.common.base.Joiner;
import com.google.common.net.HostSpecifier;
import com.google.common.net.InternetDomainName;
import io.fluo.commoncrawl.core.DataUtil;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Link {

  private static final Logger log = LoggerFactory.getLogger(Link.class);

  private URL url;
  private String anchorText;

  private Link(URL url, String anchorText) {
    this.url = url;
    this.anchorText = anchorText;
    validate();
  }

  private void validate() {
    String reformUrl = DataUtil.toUrl(getUri());
    if (!reformUrl.equals(getUrl()) && !getUrl().contains(":443")) {
      log.info("Url {} creates url {} when reformed from uri {}", getUrl(), reformUrl, getUri());
    }
  }

  public static Link fromUrl(String url, String anchorText) throws ParseException {
    try {
      URL u = new URL(url);
      if (!u.getProtocol().equalsIgnoreCase("http") && !u.getProtocol().equalsIgnoreCase("https")) {
        throw new ParseException("Bad protocol: " + u.toString(), 0);
      }
      if (u.getUserInfo() != null) {
        throw new ParseException("No user info: " + u.toString(), 0);
      }
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

  public static Link fromUrl(String url) throws ParseException {
    return fromUrl(url, "");
  }

  public static Link fromValidUrl(String url, String anchorText) {
    try {
      return fromUrl(url, anchorText);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  public static Link fromValidUrl(String url) {
    return fromValidUrl(url, "");
  }

  public static boolean isValid(String url) {
    try {
      fromUrl(url);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public String getHost() {
    return url.getHost();
  }

  public String getReverseHost() {
    return DataUtil.getReverseHost(getHost());
  }

  public String getAnchorText() {
    return anchorText;
  }

  public boolean hasIP() {
    return DataUtil.isValidIP(getHost());
  }

  public String getTopPrivate() {
    if (hasIP()) {
      return getHost();
    }
    return InternetDomainName.from(getHost()).topPrivateDomain().name();
  }

  public String getReverseTopPrivate() {
    if (hasIP()) {
      return getHost();
    }
    return DataUtil.reverseDomain(getTopPrivate());
  }

  public String getUrl() {
    return url.toString();
  }

  public String getUri() {
    return DataUtil.toUri(url);
  }

  public boolean isImage() {
    return url.getFile().matches("([^\\s]+(\\.(?i)(jpeg|jpg|png|gif|bmp))$)");
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Link) {
      Link other = (Link) o;
      return url.toString().equals(other.url.toString());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return url.toString().hashCode();
  }
}
