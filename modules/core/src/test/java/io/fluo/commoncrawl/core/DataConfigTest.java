package io.fluo.commoncrawl.core;

import org.junit.Assert;
import org.junit.Test;

public class DataConfigTest {

  @Test
  public void testBasic() throws Exception {
    DataConfig config = DataConfig.load("../../conf/data.yml.example");
    Assert.assertEquals("/path/to/fluo/install", config.fluoHome);
    Assert.assertEquals("ccrawl", config.fluoApp);
    Assert.assertEquals("ccrawl_index", config.accumuloIndexTable);
    Assert.assertEquals("/path/to/fluo/install/apps/ccrawl/conf/fluo.properties",
        config.getFluoPropsPath());
    Assert.assertEquals("/cc/data/wat", config.watDataDir);
    Assert.assertEquals("/cc/data/wet", config.wetDataDir);
    Assert.assertEquals("/cc/data/warc", config.warcDataDir);
    Assert.assertEquals("/cc/temp", config.hdfsTempDir);
    Assert.assertEquals("/path/to/hadoop/conf", config.hadoopConfDir);
  }
}
