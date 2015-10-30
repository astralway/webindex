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
    DataConfig config = DataConfig.load("../../conf/data.yml.example", false);
    Assert.assertEquals("webindex_search", config.accumuloIndexTable);
    Assert.assertEquals("https://aws-publicdatasets.s3.amazonaws.com", config.ccServerUrl);
    Assert.assertEquals("common-crawl/crawl-data/CC-MAIN-2015-18/wat.paths.gz", config.ccDataPaths);
    Assert.assertEquals("webindex", config.fluoApp);
    Assert.assertEquals("/cc/temp", config.hdfsTempDir);
    Assert.assertEquals("/cc/data/2015-18", config.hdfsDataDir);
    Assert.assertEquals("/cc/data/2015-18/init", config.getHdfsInitDir());
    Assert.assertEquals("/cc/data/2015-18/load", config.getHdfsLoadDir());
    Assert.assertEquals("/cc/data/2015-18/paths.gz", config.getHdfsPathsFile());
    Assert.assertEquals(2, config.sparkExecutorInstances);
    Assert.assertEquals("1g", config.sparkExecutorMemory);
    Assert.assertEquals(2, config.numInitFiles);
    Assert.assertEquals(2, config.numLoadFiles);
  }
}
