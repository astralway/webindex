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

import org.junit.Assert;
import org.junit.Test;

public class LinkTest {

  @Test
  public void testBasic() {
    Link link1 = Link.of("com.a>>o>/", "anchor text");
    Assert.assertEquals("http://a.com/", link1.getUrl());
    Assert.assertEquals("com.a>>o>/", link1.getUri());
    Assert.assertEquals("anchor text", link1.getAnchorText());

    Link link2 = Link.of("com.a>>o>/", "other text");
    Assert.assertEquals(link1, link2);

    Link link3 = Link.of(URLTest.from("http://a.com"), "more other text");
    Assert.assertEquals("com.a>>o>/", link3.getUri());
    Assert.assertEquals(link1, link3);
  }

}
