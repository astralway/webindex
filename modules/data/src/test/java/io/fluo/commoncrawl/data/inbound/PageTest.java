/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.commoncrawl.data.inbound;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.junit.Assert;
import org.junit.Test;

public class PageTest {

  @Test
  public void testBasic() throws IOException, ParseException {

    ArchiveReader archiveReader = WARCReaderFactory.get(new File("src/test/resources/wat.warc"));
    Page page = Page.from(archiveReader.get());

    Assert.assertEquals("application/json", page.getMimeType());
    Assert.assertEquals("http://1079ishot.com/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/", page.getLink().getUrl());
    Assert.assertEquals("com.1079ishot/presale-password-trey-songz-young-jeezy-pre-christmas-bash/screen-shot-2011-10-27-at-11-12-06-am/", page.getLink().getUri());

    Assert.assertEquals(10, page.getNumLinks());
    Assert.assertEquals(1, page.getLinks().size());
    Assert.assertEquals(0, page.getExternalLinks().size());

    ArchiveReader ar2 = WARCReaderFactory.get(new File("src/test/resources/wat-18.warc"));

    int valid = 0;
    int invalid = 0;
    Iterator<ArchiveRecord> records = ar2.iterator();
    while (records.hasNext()) {
      ArchiveRecord r = records.next();
      if (Page.isValid(r)) {
        valid++;
      } else {
        invalid++;
      }
    }
    Assert.assertEquals(17, valid);
    Assert.assertEquals(1, invalid);
  }
}
