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

public class LinkUtilTest {

  @Test
  public void testBasic() {
    Assert.assertEquals("http://thisisatest?s=a b", LinkUtil.clean(" http://this\tis\na\rtest?s=a b  "));
    Assert.assertFalse(LinkUtil.isValid("  http://test"));
    Assert.assertTrue(LinkUtil.isValid("  http://test.com"));

    Assert.assertEquals("com.example/1/2/3", LinkUtil.transform("http://example.com/1/2/3"));
    Assert.assertEquals("com.example", LinkUtil.transform("http://example.com"));
    Assert.assertEquals("com.example/", LinkUtil.transform("https://example.com/"));
    Assert.assertEquals("com.example/a", LinkUtil.transform("https:///example.com/a"));
    Assert.assertEquals("com.example/b", LinkUtil.transform("http:://example.com/b"));
    Assert.assertEquals("com.example?b", LinkUtil.transform("http:://example.com?b"));
    Assert.assertEquals("com:80.example/b", LinkUtil.transform("http:://example.com:80/b"));
    Assert.assertEquals("4.3.2.1////c", LinkUtil.transform("http://1.2.3.4////c"));

    Assert.assertTrue(LinkUtil.isImage("http://example.com/a.jpg"));
    Assert.assertTrue(LinkUtil.isImage("http://example.com/a.JPEG"));
    Assert.assertTrue(LinkUtil.isImage("http://example.com/a.png"));

    Assert.assertEquals("4.3.2.1", LinkUtil.getDomainFromUrl("http://1.2.3.4////c"));
  }

}
