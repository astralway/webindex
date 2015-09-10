/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.webindex.core;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.validator.routines.InetAddressValidator;

public class DataUtil {

  public static InetAddressValidator validator = InetAddressValidator.getInstance();

  public static String reverseDomain(String domain) {
    String[] domainArgs = domain.split("\\.");
    ArrayUtils.reverse(domainArgs);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < domainArgs.length - 1; i++) {
      sb.append(domainArgs[i]);
      sb.append(".");
    }
    sb.append(domainArgs[domainArgs.length - 1]);
    if (domain.endsWith(".")) {
      sb.append(".");
    }
    return sb.toString();
  }

  public static String toUrl(String uri) {
    String[] args = uri.split("[\\/\\?\\#]", 2);
    String[] hostArgs = args[0].split(":", 2);
    String domain = getReverseHost(hostArgs[0]);
    StringBuilder url = new StringBuilder();
    if ((hostArgs.length == 2) && !hostArgs[1].isEmpty()) {
      String proto = "http://";
      String port = hostArgs[1];
      if (hostArgs[1].startsWith("s")) {
        proto = "https://";
        port = port.substring(1);
      }
      url.append(proto + domain);
      if (!port.isEmpty()) {
        url.append(":" + port);
      }
    } else {
      url.append("http://" + domain);
    }
    if (args.length == 2) {
      int sepIndex = args[0].length();
      url.append(uri.substring(sepIndex, sepIndex + 1));
      url.append(args[1]);
    }
    return url.toString();
  }

  public static boolean isValidIP(String host) {
    return validator.isValid(host);
  }

  public static String getReverseHost(String host) {
    if (isValidIP(host)) {
      return host;
    }
    return reverseDomain(host);
  }

  public static String toUri(String url) throws MalformedURLException {
    return toUri(new URL(url));
  }

  public static String toUri(URL url) {
    StringBuilder uri = new StringBuilder();

    uri.append(getReverseHost(url.getHost()));

    if (url.getProtocol().equalsIgnoreCase("https")) {
      uri.append(":s");
      if ((url.getPort() != -1)) {
        uri.append(Integer.toString(url.getPort()));
      }
    } else if ((url.getPort() != -1)) {
      uri.append(":" + Integer.toString(url.getPort()));
    }

    StringBuilder urlStart = new StringBuilder();
    urlStart.append(url.getProtocol() + "://" + url.getHost());
    if (url.getPort() != -1) {
      urlStart.append(":" + Integer.toString(url.getPort()));
    }
    uri.append(url.toString().substring(urlStart.length()));

    return uri.toString();
  }

  public static String cleanUrl(String url) {
    String cleanUrl = url.trim();
    if (cleanUrl.length() >= 8) {
      if (cleanUrl.substring(0, 7).equalsIgnoreCase("http://")) {
        cleanUrl = "http://" + cleanUrl.substring(7);
      } else if (cleanUrl.substring(0, 8).equalsIgnoreCase("https://")) {
        cleanUrl = "https://" + cleanUrl.substring(8);
      }
    }
    return cleanUrl;
  }
}
