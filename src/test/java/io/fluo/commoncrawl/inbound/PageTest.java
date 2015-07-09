package io.fluo.commoncrawl.inbound;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {

  @Test
  public void testBasic() throws IOException {

    ArchiveReader archiveReader = WARCReaderFactory.get(new File("src/test/resources/wat.warc"));
    Page page = new Page(archiveReader.get());

    Assert.assertEquals("application/json", page.getMimeType());
    Assert.assertEquals("http://1079ishot.com/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/", page.getUrl());
    Assert.assertEquals("com.1079ishot/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/", page.getUri());

    Assert.assertEquals(10, page.getNumLinks());
    Assert.assertEquals(6, page.getUrlLinks().size());
    Assert.assertEquals(6, page.getUrlLinks().size());
    Map<String, String> links = page.getExternalUriLinks();

    Assert.assertEquals(0, page.getExternalUriLinks().size());
  }
}
