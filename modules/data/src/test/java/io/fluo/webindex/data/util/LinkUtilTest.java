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

package io.fluo.webindex.data.util;

import org.junit.Assert;
import org.junit.Test;

public class LinkUtilTest {

  @Test
  public void testBasic() throws Exception {

    // valid urls
    Assert.assertTrue(LinkUtil.isValid(" \thttp://example.com/ \t\n\r\n"));
    Assert.assertTrue(LinkUtil.isValid("http://1.2.3.4:80/test?a=b&c=d"));
    Assert.assertTrue(LinkUtil.isValid("http://1.2.3.4/"));
    Assert.assertTrue(LinkUtil.isValid("http://a.b.c.d.com/1/2/3/4/5"));
    Assert.assertTrue(LinkUtil.isValid("http://a.b.com:281/1/2"));
    Assert.assertTrue(LinkUtil.isValid("http://1.2.3.4:281/1/2"));

    // invalid urls
    Assert.assertFalse(LinkUtil.isValid("http://1.2.3:80/test?a=b&c=d"));
    Assert.assertFalse(LinkUtil.isValid("http://com/"));
    Assert.assertFalse(LinkUtil.isValid("http://test"));
    Assert.assertFalse(LinkUtil.isValid("http://co.uk"));
    Assert.assertFalse(LinkUtil.isValid("http:///example.com/"));
    Assert.assertFalse(LinkUtil.isValid("http:://example.com/"));
    Assert.assertFalse(LinkUtil.isValid("example.com"));
    Assert.assertFalse(LinkUtil.isValid("127.0.0.1"));
    Assert.assertFalse(LinkUtil.isValid("http://ab@example.com"));
    Assert.assertFalse(LinkUtil.isValid("ftp://example.com"));

    Assert.assertEquals("example.com", LinkUtil.getHost("http://example.com:281/1/2"));
    Assert.assertEquals("a.b.example.com", LinkUtil.getHost("http://a.b.example.com/1/2"));
    Assert.assertEquals("1.2.3.4", LinkUtil.getHost("http://1.2.3.4:89/1/2"));

    Assert.assertEquals("com.example", LinkUtil.getReverseHost("http://example.com:281/1"));
    Assert.assertEquals("com.example.b.a", LinkUtil.getReverseHost("http://a.b.example.com/1/2"));
    Assert.assertEquals("1.2.3.4", LinkUtil.getReverseHost("http://1.2.3.4:89/1/2"));

    Assert.assertTrue(LinkUtil.isImage("http://a.com/a.jpg"));
    Assert.assertTrue(LinkUtil.isImage("http://a.com/a.JPEG"));
    Assert.assertTrue(LinkUtil.isImage("http://a.com/c/b/a.png"));

    Assert.assertEquals("c.com", LinkUtil.getTopPrivate("http://a.b.c.com"));
    Assert.assertEquals("com.c", LinkUtil.getReverseTopPrivate("http://a.b.c.com"));
    Assert.assertEquals("c.co.uk", LinkUtil.getTopPrivate("http://a.b.c.co.uk"));
    Assert.assertEquals("uk.co.c", LinkUtil.getReverseTopPrivate("http://a.b.c.co.uk"));
    Assert.assertEquals("d.com.au", LinkUtil.getTopPrivate("http://www.d.com.au"));
    Assert.assertEquals("au.com.d", LinkUtil.getReverseTopPrivate("http://www.d.com.au"));
  }
}
