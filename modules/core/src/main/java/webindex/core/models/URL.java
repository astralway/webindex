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
import java.util.function.Function;

import com.google.common.net.HostSpecifier;
import com.google.common.net.InternetDomainName;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URL implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(URL.class);

  private static final String URL_SEP_REGEX = "[/?#]";
  private static final String HTTP_PROTO = "http://";
  private static final String HTTPS_PROTO = "https://";
  private static final String URI_SEP = ">";
  public static final InetAddressValidator validator = InetAddressValidator.getInstance();

  private static final long serialVersionUID = 1L;

  private String domain;
  private String host;
  private String path;
  private int port;
  private boolean secure;
  private boolean ipHost;

  public URL(String domain, String host, String path, int port, boolean secure, boolean ipHost) {
    Objects.requireNonNull(domain);
    Objects.requireNonNull(host);
    Objects.requireNonNull(path);
    this.domain = domain;
    this.host = host;
    this.path = path;
    this.port = port;
    this.secure = secure;
    this.ipHost = ipHost;
  }

  public static void badUrl(boolean logError, String msg) {
    if (logError) {
      log.error(msg);
    } else {
      log.debug(msg);
    }
    throw new IllegalArgumentException(msg);
  }

  public static String domainFromHost(String host) {
    return InternetDomainName.from(host).topPrivateDomain().name();
  }

  public static boolean isValidHost(String host) {
    return HostSpecifier.isValid(host) && InternetDomainName.isValid(host)
        && InternetDomainName.from(host).isUnderPublicSuffix();
  }

  public static URL from(String rawUrl) {
    return URL.from(rawUrl, URL::domainFromHost, URL::isValidHost);
  }

  public static URL from(String rawUrl, Function<String, String> domainFromHost,
      Function<String, Boolean> isValidHost) {

    if (rawUrl.contains(URI_SEP)) {
      badUrl(false, "Skipping raw URL as it contains '" + URI_SEP + "':" + rawUrl);
    }

    String trimUrl = rawUrl.trim();
    if (trimUrl.length() < 8) {
      badUrl(false, "Raw URL is too short to start with valid protocol: " + rawUrl);
    }

    String urlNoProto = "";
    boolean secure = false;
    int port = 80;
    if (trimUrl.substring(0, 7).equalsIgnoreCase(HTTP_PROTO)) {
      urlNoProto = trimUrl.substring(7);
    } else if (trimUrl.substring(0, 8).equalsIgnoreCase(HTTPS_PROTO)) {
      urlNoProto = trimUrl.substring(8);
      secure = true;
      port = 443;
    } else {
      badUrl(false, "Raw URL does not start with valid protocol: " + rawUrl);
    }

    String hostPort;
    String[] args = urlNoProto.split(URL_SEP_REGEX, 2);
    String path;
    String sep;
    if (args.length == 2) {
      hostPort = args[0].toLowerCase();
      int sepIndex = args[0].length();
      sep = urlNoProto.substring(sepIndex, sepIndex + 1);
      path = sep + args[1];
    } else {
      hostPort = urlNoProto.toLowerCase();
      path = "/";
    }

    args = hostPort.split(":", 2);
    String host;
    if (args.length == 2) {
      host = args[0];
      try {
        port = Integer.parseInt(args[1]);
      } catch (NumberFormatException e) {
        badUrl(false, "Raw URL (" + rawUrl + ") has invalid port: " + args[1]);
      }
    } else {
      host = hostPort;
    }

    if (host.isEmpty()) {
      badUrl(false, "Raw URL cannot have empty host: " + rawUrl);
    }

    String domain = host;
    boolean ipHost = isValidIP(host);
    if (!ipHost) {
      if (!isValidHost.apply(host)) {
        badUrl(false, "Raw URL (" + rawUrl + ") has invalid host: " + host);
      }
      domain = domainFromHost.apply(host);
    }

    return new URL(domain, host, path, port, secure, ipHost);
  }

  public static boolean isValid(String rawUrl) {
    return URL.isValid(rawUrl, URL::domainFromHost, URL::isValidHost);
  }

  public static boolean isValid(String rawUrl, Function<String, String> domainFromHost,
      Function<String, Boolean> isValidHost) {
    try {
      from(rawUrl, domainFromHost, isValidHost);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isValidIP(String host) {
    return validator.isValid(host);
  }

  public static String reverseHost(String host) {
    String[] hostArgs = host.split("\\.");
    ArrayUtils.reverse(hostArgs);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < hostArgs.length - 1; i++) {
      sb.append(hostArgs[i]);
      sb.append(".");
    }
    sb.append(hostArgs[hostArgs.length - 1]);
    if (host.endsWith(".")) {
      sb.append(".");
    }
    return sb.toString();
  }

  public boolean hasIPHost() {
    return ipHost;
  }

  public String getHost() {
    return host;
  }

  public String getReverseHost() {
    if (hasIPHost()) {
      return host;
    }
    return reverseHost(host);
  }

  public String getPath() {
    return path;
  }

  public boolean isSecure() {
    return secure;
  }

  public int getPort() {
    return port;
  }

  public boolean isImage() {
    return path.matches("([^\\s]+(\\.(?i)(jpeg|jpg|png|gif|bmp))$)");
  }

  @Override
  public String toString() {
    StringBuilder url = new StringBuilder();
    url.append("http");
    if (secure) {
      url.append("s");
    }
    url.append("://");
    url.append(host);
    if (!(port == 80 && !secure) && !(port == 443 && secure)) {
      url.append(":");
      url.append(port);
    }
    url.append(path);
    return url.toString();
  }

  public String toUri() {
    String reverseDomain = getReverseDomain();
    String nonDomain = getReverseHost().substring(reverseDomain.length());
    String portStr = "";
    if ((!secure && port != 80) || (secure && port != 443)) {
      portStr = Integer.toString(port);
    }
    return reverseDomain + URI_SEP + nonDomain + URI_SEP + (secure ? "s" : "o") + portStr + URI_SEP
        + path;
  }

  public static URL fromUri(String uri) {
    String[] idArgs = uri.split(URI_SEP);
    if (idArgs.length != 4) {
      throw new IllegalArgumentException("Page ID has too few or many parts: " + uri);
    }
    String domain = idArgs[0];
    String host = idArgs[0] + idArgs[1];
    boolean ipHost = isValidIP(host);
    if (!ipHost) {
      domain = reverseHost(domain);
      host = reverseHost(host);
    }
    boolean secure = false;
    int port = 80;
    if (idArgs[2].startsWith("s")) {
      secure = true;
      port = 443;
    } else if (!idArgs[2].startsWith("o")) {
      throw new IllegalArgumentException("Page ID does not have port info beg with 's' or 'o': "
          + uri);
    }
    if (idArgs[2].length() > 1) {
      port = Integer.parseInt(idArgs[2].substring(1));
    }
    String path = idArgs[3];
    return new URL(domain, host, path, port, secure, ipHost);
  }

  public String getDomain() {
    return domain;
  }

  public String getReverseDomain() {
    if (hasIPHost()) {
      return domain;
    }
    return reverseHost(domain);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof URL) {
      URL other = (URL) o;
      return domain.equals(other.domain) && host.equals(other.host) && path.equals(other.path)
          && port == other.port && secure == other.secure;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = domain.hashCode();
    result = 31 * result + host.hashCode();
    result = 31 * result + path.hashCode();
    result = 31 * result + port;
    result = 31 * result + (secure ? 1 : 0);
    return result;
  }
}
