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

import com.google.common.net.HostSpecifier;
import com.google.common.net.InternetDomainName;
import io.fluo.commoncrawl.core.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkUtil {

  private static final Logger log = LoggerFactory.getLogger(LinkUtil.class);

  /*
  private URL url;
  private String anchorText;

  private LinkUtil(URL url, String anchorText) {
    this.url = url;
    this.anchorText = anchorText;
    validate();
  }*/

  public static URL createURL(String url) throws ParseException {
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
      String initialUrl = url.trim();
      String uri = DataUtil.toUri(url);
      String reformUrl = DataUtil.toUrl(uri);
      if (!reformUrl.equals(initialUrl) && !initialUrl.contains(":443")) {
        log.info("Url {} creates url {} when reformed from uri {}", url, reformUrl, uri);
      }
      return u;
    } catch (MalformedURLException e) {
      throw new ParseException(e.getMessage(), 0);
    }
  }

  public static void validate(String url) throws ParseException {
    createURL(url);
  }

  public static boolean isValid(String url) {
    try {
      validate(url);
      return true;
    } catch (ParseException e) {
      return false;
    }
  }

  public static String getHost(String url) throws ParseException {
    URL u = createURL(url);
    return u.getHost();
  }

  public static String getReverseHost(String url) throws ParseException {
    return DataUtil.getReverseHost(getHost(url));
  }

  public static boolean hasIP(String url) throws ParseException {
    return DataUtil.isValidIP(getHost(url));
  }

  public static String getTopPrivate(String url) throws ParseException {
    if (hasIP(url)) {
      return getHost(url);
    }
    return InternetDomainName.from(getHost(url)).topPrivateDomain().name();
  }

  public static String getReverseTopPrivate(String url) throws ParseException {
    if (hasIP(url)) {
      return getHost(url);
    }
    return DataUtil.reverseDomain(getTopPrivate(url));
  }

  public static boolean isImage(String url) throws ParseException {
    URL u = createURL(url);
    return u.getFile().matches("([^\\s]+(\\.(?i)(jpeg|jpg|png|gif|bmp))$)");
  }
}
