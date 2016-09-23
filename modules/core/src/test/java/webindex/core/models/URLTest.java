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

import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

public class URLTest {

  public static URL from(String rawUrl) {
    return URL.from(rawUrl);
  }

  public static String toID(String rawUrl) {
    return from(rawUrl).toUri();
  }


  public static URL url80(String host, String path) {
    return new URL(URL.domainFromHost(host), host, path, 80, false, URL.isValidIP(host));
  }

  public static URL url443(String host, String path) {
    return new URL(URL.domainFromHost(host), host, path, 443, true, URL.isValidIP(host));
  }

  public static URL urlOpen(String host, String path, int port) {
    return new URL(URL.domainFromHost(host), host, path, port, false, URL.isValidIP(host));
  }

  public static URL urlSecure(String host, String path, int port) {
    return new URL(URL.domainFromHost(host), host, path, port, true, URL.isValidIP(host));
  }

  @Test
  public void testBasic() throws ParseException {

    String[] validUrls =
        {"http://ab.com/", "https://ab.com/1/2/3", "https://ab.com:8080?1/2/3",
            "http://ab.com#1/2/3", "https://ab.com/", "https://h.d.ab.com/1/2/3"};

    for (String rawUrl : validUrls) {
      Assert.assertTrue(URL.isValid(rawUrl));
      Assert.assertEquals(rawUrl, from(rawUrl).toString());
    }

    String[] failureUrls =
        {"ab.com", "ab.com/1/2/3", "htttp://ab.com/", "httpss://ab.com/", "http:/ab.com/",
            "http::/ab.com/", "http:///ab.com/", "hhttp://ab.com/", "http://a.com:/test/",
            "http://a.com:"};

    for (String rawUrl : failureUrls) {
      Assert.assertFalse(URL.isValid(rawUrl));
    }
  }

  @Test
  public void testClean() {
    Assert.assertEquals("http://example.com/", from("Http://example.com  ").toString());
    Assert.assertEquals("https://example.com/", from(" HTTPS://example.com/ ").toString());
    Assert.assertEquals("http://a.b.com:281/a/b", from("http://A.B.Com:281/a/b").toString());
    Assert.assertEquals("http://a.b.com:281/A/b", from("http://A.b.Com:281/A/b").toString());
    Assert.assertEquals("http://a.b.com?A/b/C", from("http://a.B.Com?A/b/C").toString());
    Assert.assertEquals("http://a.be.com/", from("http://A.Be.COM").toString());
  }

  @Test
  public void testPort() {
    Assert.assertEquals(80, from("http://www.ab.com:80/").getPort());
    Assert.assertEquals("www.ab.com", from("http://www.ab.com:80/").getHost());
    Assert.assertEquals("http://www.ab.com/", from("http://www.ab.com:80/").toString());
    Assert.assertEquals(443, from("https://ab.com/").getPort());
    Assert.assertTrue(from("https://ab.com/").isSecure());
    Assert.assertEquals("www.ab.com", from("https://www.ab.com:443/").getHost());
    Assert.assertEquals("https://www.ab.com/", from("https://www.ab.com:443/").toString());
    Assert.assertEquals(8888, from("https://ab.com:8888/").getPort());
    Assert.assertEquals("www.ab.com", from("https://www.ab.com:8888/").getHost());
    Assert.assertEquals("http://www.ab.com:8888/", from("http://www.ab.com:8888/").toString());
  }

  @Test
  public void testHost() {
    URL u = from("http://a.b.c.d.com/1/2/3");
    Assert.assertEquals("a.b.c.d.com", u.getHost());
    Assert.assertEquals("com.d.c.b.a", u.getReverseHost());
    Assert.assertEquals("d.com", u.getDomain());
    Assert.assertEquals("com.d", u.getReverseDomain());
  }

  @Test
  public void testAdvanced() {
    Assert.assertEquals(urlOpen("example.com", "?A&B", 83), from("http://EXAMPLE.COM:83?A&B"));
    Assert.assertEquals(urlOpen("example.com", "#a&b", 83), from("http://example.com:83#a&b"));
    Assert.assertEquals(url80("a.b.example.com", "/page?1&2"),
        from("http://a.b.example.com/page?1&2"));
    Assert.assertEquals(url80("example.com", "/1/2/3?c&d&e"),
        from("http://example.com/1/2/3?c&d&e"));
    Assert.assertEquals(url80("a.b.example.com", "/"), from("http://a.b.example.com"));
    Assert.assertEquals(url443("a.b.example.com", "/"), from("https://A.b.example.com/"));
    Assert.assertEquals(urlSecure("a.b.example.com", "/", 8329),
        from("https://a.b.Example.com:8329/"));
    Assert
        .assertEquals(urlOpen("a.b.example.com", "/", 8333), from("http://a.B.example.com:8333/"));
    Assert.assertEquals(url443("example.com", "/"), from("https://example.com/"));
    Assert.assertEquals(url80("example.com", "/b?1#2&3#4"), from("http://example.com/b?1#2&3#4"));
    Assert.assertEquals(urlOpen("example.com", "/b", 8080), from("http://example.com:8080/b"));
  }

  @Test
  public void testId() {
    URL u1 = urlSecure("a.b.c.com", "/", 8329);
    URL u2 = from("https://a.b.C.com:8329");
    String r1 = u2.toUri();
    Assert.assertEquals("com.c>.b.a>s8329>/", r1);
    URL u3 = URL.fromUri(r1);
    Assert.assertEquals(u1, u2);
    Assert.assertEquals(u1, u3);
    Assert.assertEquals(u2, u3);

    URL u4 = url80("d.com", "/a/b/c");
    String id4 = u4.toUri();
    Assert.assertEquals("com.d>>o>/a/b/c", id4);
    Assert.assertEquals(u4, URL.fromUri(id4));

    URL u5 = from("http://1.2.3.4/a/b/c");
    String id5 = u5.toUri();
    Assert.assertEquals("1.2.3.4>>o>/a/b/c", id5);
    Assert.assertEquals(u5, URL.fromUri(id5));

    Assert.assertEquals("com.b>.a>s80>/", from("https://a.b.com:80").toUri());
  }

  @Test
  public void testMore() throws Exception {

    // valid urls
    Assert.assertTrue(URL.isValid(" \thttp://example.com/ \t\n\r\n"));
    Assert.assertTrue(URL.isValid("http://1.2.3.4:80/test?a=b&c=d"));
    Assert.assertTrue(URL.isValid("http://1.2.3.4/"));
    Assert.assertTrue(URL.isValid("http://a.b.c.d.com/1/2/3/4/5"));
    Assert.assertTrue(URL.isValid("http://a.b.com:281/1/2"));
    Assert.assertTrue(URL.isValid("http://A.B.Com:281/a/b"));
    Assert.assertTrue(URL.isValid("http://A.b.Com:281/A/b"));
    Assert.assertTrue(URL.isValid("http://a.B.Com?A/b/C"));
    Assert.assertTrue(URL.isValid("http://A.Be.COM"));
    Assert.assertTrue(URL.isValid("http://1.2.3.4:281/1/2"));

    // invalid urls
    Assert.assertFalse(URL.isValid("http://a.com:/test"));
    Assert.assertFalse(URL.isValid("http://z.com:"));
    Assert.assertFalse(URL.isValid("http://1.2.3:80/test?a=b&c=d"));
    Assert.assertFalse(URL.isValid("http://1.2.3/"));
    Assert.assertFalse(URL.isValid("http://com/"));
    Assert.assertFalse(URL.isValid("http://a.b.c.com/bad>et"));
    Assert.assertFalse(URL.isValid("http://test"));
    Assert.assertFalse(URL.isValid("http://co.uk"));
    Assert.assertFalse(URL.isValid("http:///example.com/"));
    Assert.assertFalse(URL.isValid("http:://example.com/"));
    Assert.assertFalse(URL.isValid("example.com"));
    Assert.assertFalse(URL.isValid("127.0.0.1"));
    Assert.assertFalse(URL.isValid("http://ab@example.com"));
    Assert.assertFalse(URL.isValid("ftp://example.com"));

    Assert.assertEquals("example.com", from("http://example.com:281/1/2").getHost());
    Assert.assertEquals("a.b.example.com", from("http://a.b.example.com/1/2").getHost());
    Assert.assertEquals("a.b.example.com", from("http://A.B.Example.Com/1/2").getHost());
    Assert.assertEquals("1.2.3.4", from("http://1.2.3.4:89/1/2").getHost());

    Assert.assertEquals("/A/b/C", from("http://A.B.Example.Com/A/b/C").getPath());
    Assert.assertEquals("?D/E/f", from("http://A.B.Example.Com?D/E/f").getPath());

    URL u = from("http://a.b.c.d.com/1/2/3");
    Assert.assertEquals("a.b.c.d.com", u.getHost());
    Assert.assertEquals("com.d.c.b.a", u.getReverseHost());
    Assert.assertEquals("d.com", u.getDomain());
    Assert.assertEquals("com.d", u.getReverseDomain());

    Assert.assertEquals("com.example", from("http://example.com:281/1").getReverseHost());
    Assert.assertEquals("com.example.b.a", from("http://a.b.example.com/1/2").getReverseHost());
    Assert.assertEquals("1.2.3.4", from("http://1.2.3.4:89/1/2").getReverseHost());

    Assert.assertTrue(from("http://a.com/a.jpg").isImage());
    Assert.assertTrue(from("http://a.com/a.JPEG").isImage());
    Assert.assertTrue(from("http://a.com/c/b/a.png").isImage());

    Assert.assertEquals("c.com", from("http://a.b.c.com").getDomain());
    Assert.assertEquals("com.c", from("http://a.b.c.com").getReverseDomain());
    Assert.assertEquals("c.co.uk", from("http://a.b.c.co.uk").getDomain());
    Assert.assertEquals("uk.co.c", from("http://a.b.c.co.uk").getReverseDomain());
    Assert.assertEquals("d.com.au", from("http://www.d.com.au").getDomain());
    Assert.assertEquals("au.com.d", from("http://www.d.com.au").getReverseDomain());

    u = from("https://www.d.com.au:9443/a/bc");
    Assert.assertEquals("au.com.d>.www>s9443>/a/bc", u.toUri());
    Assert.assertEquals("https://www.d.com.au:9443/a/bc", u.toString());
    URL u2 = URL.fromUri(u.toUri());
    Assert.assertEquals("https://www.d.com.au:9443/a/bc", u2.toString());
    Assert.assertEquals("d.com.au", u2.getDomain());
    Assert.assertEquals("www.d.com.au", u2.getHost());
  }
}
