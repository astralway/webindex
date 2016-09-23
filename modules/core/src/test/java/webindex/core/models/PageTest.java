/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {

  @Test
  public void testBasic() {

    Page page = new Page(URLTest.from("http://example.com").toUri());
    Assert.assertEquals("http://example.com/", page.getUrl());
    Assert.assertEquals("com.example>>o>/", page.getUri());
    Assert.assertEquals(Long.valueOf(0), page.getNumOutbound());
    Assert.assertTrue(page.addOutbound(Link.of(URLTest.from("http://test1.com"), "test1")));
    Assert.assertEquals(Long.valueOf(1), page.getNumOutbound());
    Assert.assertTrue(page.addOutbound(Link.of(URLTest.from("http://test2.com"), "test2")));
    Assert.assertEquals(Long.valueOf(2), page.getNumOutbound());
    Assert.assertFalse(page.addOutbound(Link.of(URLTest.from("http://test2.com"), "test1234")));
    Assert.assertEquals(Long.valueOf(2), page.getNumOutbound());

    Gson gson = new Gson();
    String json = gson.toJson(page);
    Assert.assertNotNull(json);
    Assert.assertFalse(json.isEmpty());

    Page after = gson.fromJson(json, Page.class);
    Assert.assertEquals(page.getUrl(), after.getUrl());
    Assert.assertEquals(page.getOutboundLinks().size(), after.getOutboundLinks().size());
  }
}
