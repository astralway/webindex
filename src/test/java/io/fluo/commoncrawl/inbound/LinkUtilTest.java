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
