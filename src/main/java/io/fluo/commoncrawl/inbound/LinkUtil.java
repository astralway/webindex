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
