package io.fluo.commoncrawl.core;

import org.junit.Assert;
import org.junit.Test;

public class DataUtilTest {

  @Test
  public void testBasic() throws Exception {

    Assert.assertEquals("http://a.b.example.com/page?1&2", DataUtil.toUrl("com.example.b.a/page?1&2"));
    Assert.assertEquals("https://a.b.example.com/page?1&2", DataUtil.toUrl("com.example.b.a:443/page?1&2"));
    Assert.assertEquals("http://a.b.example.com:8080/page?1&2", DataUtil.toUrl("com.example.b.a:8080/page?1&2"));
    Assert.assertEquals("http://b.example.com", DataUtil.toUrl("com.example.b"));
    Assert.assertEquals("http://b.example.com/", DataUtil.toUrl("com.example.b/"));

    Assert.assertEquals("com.example.b.a/page?1&2", DataUtil.toUri("http://a.b.example.com/page?1&2"));
    Assert.assertEquals("1.2.3.4:443/page?1&2", DataUtil.toUri("https://1.2.3.4/page?1&2"));
    Assert.assertEquals("1.2.3.4/page?1&2", DataUtil.toUri("http://1.2.3.4/page?1&2"));
  }
}
