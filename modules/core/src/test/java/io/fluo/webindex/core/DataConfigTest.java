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

package io.fluo.webindex.core;

import org.junit.Assert;
import org.junit.Test;

public class DataConfigTest {

  @Test
  public void testBasic() throws Exception {
    DataConfig config = DataConfig.load("../../conf/data.yml.example");
    Assert.assertEquals("/path/to/fluo/install", config.fluoHome);
    Assert.assertEquals("webindex", config.fluoApp);
    Assert.assertEquals("webindex_search", config.accumuloIndexTable);
    Assert.assertEquals("/path/to/fluo/install/apps/webindex/conf/fluo.properties",
        config.getFluoPropsPath());
    Assert.assertEquals("/cc/data/wat", config.watDataDir);
    Assert.assertEquals("/cc/data/wet", config.wetDataDir);
    Assert.assertEquals("/cc/data/warc", config.warcDataDir);
    Assert.assertEquals("/cc/temp", config.hdfsTempDir);
    Assert.assertEquals("/path/to/hadoop/conf", config.hadoopConfDir);
  }
}
