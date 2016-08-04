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

package io.fluo.webindex.data.util;

import io.fluo.webindex.core.models.URL;
import org.junit.Assert;
import org.junit.Test;

public class DataUrlTest {

  public static URL build(String rawUrl) {
    return DataUrl.from(rawUrl);
  }

  public static boolean isValid(String rawUrl) {
    return DataUrl.isValid(rawUrl);
  }

  @Test
  public void testBasic() throws Exception {

    // valid urls
    Assert.assertTrue(isValid(" \thttp://example.com/ \t\n\r\n"));
    Assert.assertTrue(isValid("http://1.2.3.4:80/test?a=b&c=d"));
    Assert.assertTrue(isValid("http://1.2.3.4/"));
    Assert.assertTrue(isValid("http://a.b.c.d.com/1/2/3/4/5"));
    Assert.assertTrue(isValid("http://a.b.com:281/1/2"));
    Assert.assertTrue(isValid("http://A.B.Com:281/a/b"));
    Assert.assertTrue(isValid("http://A.b.Com:281/A/b"));
    Assert.assertTrue(isValid("http://a.B.Com?A/b/C"));
    Assert.assertTrue(isValid("http://A.Be.COM"));
    Assert.assertTrue(isValid("http://1.2.3.4:281/1/2"));

    // invalid urls
    Assert.assertFalse(isValid("http://a.com:/test"));
    Assert.assertFalse(isValid("http://z.com:"));
    Assert.assertFalse(isValid("http://1.2.3:80/test?a=b&c=d"));
    Assert.assertFalse(isValid("http://1.2.3/"));
    Assert.assertFalse(isValid("http://com/"));
    Assert.assertFalse(isValid("http://a.b.c.com/bad>et"));
    Assert.assertFalse(isValid("http://test"));
    Assert.assertFalse(isValid("http://co.uk"));
    Assert.assertFalse(isValid("http:///example.com/"));
    Assert.assertFalse(isValid("http:://example.com/"));
    Assert.assertFalse(isValid("example.com"));
    Assert.assertFalse(isValid("127.0.0.1"));
    Assert.assertFalse(isValid("http://ab@example.com"));
    Assert.assertFalse(isValid("ftp://example.com"));

    Assert.assertEquals("example.com", build("http://example.com:281/1/2").getHost());
    Assert.assertEquals("a.b.example.com", build("http://a.b.example.com/1/2").getHost());
    Assert.assertEquals("a.b.example.com", build("http://A.B.Example.Com/1/2").getHost());
    Assert.assertEquals("1.2.3.4", build("http://1.2.3.4:89/1/2").getHost());

    Assert.assertEquals("/A/b/C", build("http://A.B.Example.Com/A/b/C").getPath());
    Assert.assertEquals("?D/E/f", build("http://A.B.Example.Com?D/E/f").getPath());

    URL u = build("http://a.b.c.d.com/1/2/3");
    Assert.assertEquals("a.b.c.d.com", u.getHost());
    Assert.assertEquals("com.d.c.b.a", u.getReverseHost());
    Assert.assertEquals("d.com", u.getDomain());
    Assert.assertEquals("com.d", u.getReverseDomain());

    Assert.assertEquals("com.example", build("http://example.com:281/1").getReverseHost());
    Assert.assertEquals("com.example.b.a", build("http://a.b.example.com/1/2").getReverseHost());
    Assert.assertEquals("1.2.3.4", build("http://1.2.3.4:89/1/2").getReverseHost());

    Assert.assertTrue(build("http://a.com/a.jpg").isImage());
    Assert.assertTrue(build("http://a.com/a.JPEG").isImage());
    Assert.assertTrue(build("http://a.com/c/b/a.png").isImage());

    Assert.assertEquals("c.com", build("http://a.b.c.com").getDomain());
    Assert.assertEquals("com.c", build("http://a.b.c.com").getReverseDomain());
    Assert.assertEquals("c.co.uk", build("http://a.b.c.co.uk").getDomain());
    Assert.assertEquals("uk.co.c", build("http://a.b.c.co.uk").getReverseDomain());
    Assert.assertEquals("d.com.au", build("http://www.d.com.au").getDomain());
    Assert.assertEquals("au.com.d", build("http://www.d.com.au").getReverseDomain());

    u = build("https://www.d.com.au:9443/a/bc");
    Assert.assertEquals("au.com.d>.www>s9443>/a/bc", u.toPageID());
    Assert.assertEquals("https://www.d.com.au:9443/a/bc", u.toString());
    URL u2 = URL.fromPageID(u.toPageID());
    Assert.assertEquals("https://www.d.com.au:9443/a/bc", u2.toString());
    Assert.assertEquals("d.com.au", u2.getDomain());
    Assert.assertEquals("www.d.com.au", u2.getHost());
  }
}
