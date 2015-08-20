package io.fluo.commoncrawl.web.util;

import java.net.MalformedURLException;
import java.net.URL;

import com.google.common.net.InternetDomainName;

public class WebUtil {

  public static String getDomain(String url) {
    try {
      URL u = new URL(url);
      return InternetDomainName.from(u.getHost()).topPrivateDomain().toString();
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return "unknown";
    }
  }

}
