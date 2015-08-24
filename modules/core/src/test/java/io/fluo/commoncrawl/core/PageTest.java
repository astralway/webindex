package io.fluo.commoncrawl.core;

import com.google.gson.Gson;
import io.fluo.commoncrawl.core.models.Page;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {

  @Test
  public void testBasic() {

    Page page = new Page("http://example.com");
    Assert.assertEquals(new Long(0), page.getNumOutbound());
    page.addOutboundLink("http://test1.com", "test1");
    Assert.assertEquals(new Long(1), page.getNumOutbound());
    page.addOutboundLink("http://test2.com", "test2");
    Assert.assertEquals(new Long(2), page.getNumOutbound());
    page.addOutboundLink("http://test2.com", "test1234");
    Assert.assertEquals(new Long(2), page.getNumOutbound());

    Gson gson = new Gson();
    String json = gson.toJson(page);
    Assert.assertNotNull(json);
    Assert.assertFalse(json.isEmpty());

    Page after = gson.fromJson(json, Page.class);
    Assert.assertEquals(page.getUrl(), after.getUrl());
    Assert.assertEquals(page.getOutboundLinks().size(), after.getOutboundLinks().size());
  }
}
