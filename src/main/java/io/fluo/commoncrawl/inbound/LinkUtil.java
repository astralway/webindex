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

package io.fluo.commoncrawl.inbound;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.ArrayUtils;

public class LinkUtil {

  static Pattern urlPat = Pattern.compile("^https?:+/+([\\w\\-]+\\.[\\w\\-\\.:]+)([\\?/]+.*)?$");

  public static String clean(String url) {
    return url.trim().replaceAll("[\t\r\n]", "");
  }

  public static String transform(String url) {
    Matcher matcher = urlPat.matcher(clean(url));
    if (matcher.find()) {
      String domain = matcher.group(1);
      String extra = matcher.group(2);
      String[] domainArgs = domain.split("\\.");
      ArrayUtils.reverse(domainArgs);
      String revDomain = Joiner.on(".").join(domainArgs);
      return revDomain + (extra != null ? extra : "");
    } else {
      throw new IllegalArgumentException("Bad url: "+ url);
    }
  }

  public static String getDomainFromUri(String uri) {
    return uri.split("[\\?/]")[0];
  }

  public static String getDomainFromUrl(String url) {
    return getDomainFromUri(transform(url));
  }

  public static boolean isValid(String url) {
    try {
      transform(url);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isImage(String url) {
    return url.matches("([^\\s]+(\\.(?i)(jpeg|jpg|png|gif|bmp))$)");
  }
}
