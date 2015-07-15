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

import org.junit.Assert;
import org.junit.Test;

public class LinkTest {

  @Test
  public void testBasic() {

    // valid urls
    Assert.assertTrue(Link.isValid(" \thttp://example.com/ \t\n\r\n"));
    Assert.assertTrue(Link.isValid("http://1.2.3.4:80/test?a=b&c=d"));
    Assert.assertTrue(Link.isValid("http://1.2.3.4/"));
    Assert.assertTrue(Link.isValid("http://a.b.c.d.com/1/2/3/4/5"));

    // invalid urls
    Assert.assertFalse(Link.isValid("http://1.2.3:80/test?a=b&c=d"));
    Assert.assertFalse(Link.isValid("http://com/"));
    Assert.assertFalse(Link.isValid("http://test"));
    Assert.assertFalse(Link.isValid("http://co.uk"));
    Assert.assertFalse(Link.isValid("http:///example.com/"));
    Assert.assertFalse(Link.isValid("http:://example.com/"));
    Assert.assertFalse(Link.isValid("example.com"));
    Assert.assertFalse(Link.isValid("127.0.0.1"));

    Assert.assertEquals("http://a.b.com:281/1/2", Link.fromValid("http://a.b.com:281/1/2").getUrl());
    Assert.assertEquals("http://1.2.3.4:281/1/2", Link.fromValid("http://1.2.3.4:281/1/2").getUrl());

    Assert.assertEquals("example.com", Link.fromValid("http://example.com:281/1/2").getHost());
    Assert.assertEquals("a.b.example.com", Link.fromValid("http://a.b.example.com/1/2").getHost());
    Assert.assertEquals("1.2.3.4", Link.fromValid("https://1.2.3.4:89/1/2").getHost());

    Assert.assertEquals("com.example", Link.fromValid("http://example.com:281/1").getReverseHost());
    Assert.assertEquals("com.example.b.a", Link.fromValid("http://a.b.example.com/1/2").getReverseHost());
    Assert.assertEquals("1.2.3.4", Link.fromValid("https://1.2.3.4:89/1/2").getReverseHost());

    Assert.assertEquals("com.example/1/2/3?c&d&e", Link.fromValid("http://example.com/1/2/3?c&d&e").getUri());
    Assert.assertEquals("com.example.b.a", Link.fromValid("http://a.b.example.com").getUri());
    Assert.assertEquals("com.example/", Link.fromValid("https://example.com/").getUri());
    Assert.assertEquals("com.example?b", Link.fromValid("http://example.com?b").getUri());
    Assert.assertEquals("com.example/b", Link.fromValid("http://example.com:80/b").getUri());
    Assert.assertEquals("com.example:8080/b", Link.fromValid("http://example.com:8080/b").getUri());
    Assert.assertEquals("1.2.3.4////c", Link.fromValid("http://1.2.3.4////c").getUri());

    Assert.assertTrue(Link.fromValid("http://a.com/a.jpg").isImage());
    Assert.assertTrue(Link.fromValid("http://a.com/a.JPEG").isImage());
    Assert.assertTrue(Link.fromValid("http://a.com/c/b/a.png").isImage());

    Assert.assertEquals("c.com", Link.fromValid("http://a.b.c.com").getTopPrivate());
    Assert.assertEquals("com.c", Link.fromValid("http://a.b.c.com").getReverseTopPrivate());
    Assert.assertEquals("c.co.uk", Link.fromValid("http://a.b.c.co.uk").getTopPrivate());
    Assert.assertEquals("uk.co.c", Link.fromValid("http://a.b.c.co.uk").getReverseTopPrivate());

  }
}
