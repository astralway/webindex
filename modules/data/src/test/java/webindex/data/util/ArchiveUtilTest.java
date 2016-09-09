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

package webindex.data.util;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.Assert;
import org.junit.Test;
import webindex.core.models.Page;

public class ArchiveUtilTest {

  @Test
  public void testBasic() throws IOException, ParseException {

    ArchiveReader archiveReader = WARCReaderFactory.get(new File("src/test/resources/wat.warc"));
    Page page = ArchiveUtil.buildPage(archiveReader.get());
    Assert.assertNotNull(page);
    Assert.assertFalse(page.isEmpty());

    Assert
        .assertEquals(
            "http://1079ishot.com/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/",
            page.getUrl());
    Assert
        .assertEquals(
            "com.1079ishot>>o>/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/",
            page.getUri());

    Assert.assertEquals("2015-04-18T03:35:13Z", page.getCrawlDate());
    Assert.assertEquals("nginx/1.6.2", page.getServer());
    Assert
        .assertEquals(
            "Presale Password &#8211; Trey Songz &#038; Young Jeezy Pre-Christmas Bash Screen shot 2011-10-27 at ",
            page.getTitle());
    Assert.assertEquals(0, page.getOutboundLinks().size());

    ArchiveReader ar2 = WARCReaderFactory.get(new File("src/test/resources/wat-18.warc"));

    int valid = 0;
    int invalid = 0;
    Iterator<ArchiveRecord> records = ar2.iterator();
    while (records.hasNext()) {
      try {
        ArchiveRecord r = records.next();
        ArchiveUtil.buildPage(r);
        valid++;
      } catch (ParseException e) {
        invalid++;
      }
    }
    Assert.assertEquals(18, valid);
    Assert.assertEquals(0, invalid);
  }
}
