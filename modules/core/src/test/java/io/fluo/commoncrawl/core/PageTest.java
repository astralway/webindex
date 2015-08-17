package io.fluo.commoncrawl.core;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {

  @Test
  public void testBasic() {

    Page page = new Page("http://example.com");
    Assert.assertEquals(0, page.getExternalLinks().size());
    page.addExternalLink("http://test1.com", "test1");
    page.addExternalLink("http://test2.com", "test2");
    Assert.assertEquals(2, page.getExternalLinks().size());
    page.addExternalLink("http://test2.com", "test1234");
    Assert.assertEquals(2, page.getExternalLinks().size());

    Gson gson = new Gson();
    String json = gson.toJson(page);
    Assert.assertNotNull(json);
    Assert.assertFalse(json.isEmpty());

    Page after = gson.fromJson(json, Page.class);
    Assert.assertEquals(page.getPageUrl(), after.getPageUrl());
    Assert.assertEquals(page.getExternalLinks().size(), after.getExternalLinks().size());
  }
}
