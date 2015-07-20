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

package io.fluo.commoncrawl.mapred;

import java.io.IOException;
import java.util.Set;

import io.fluo.commoncrawl.inbound.Link;
import io.fluo.commoncrawl.inbound.Page;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class LinkMap {

  private static final Logger LOG = Logger.getLogger(LinkMap.class);

  protected enum MAPCOUNTER {
    PAGES,
    PAGES_BAD_URL,
    PAGES_NO_LINKS,
    PAGES_NO_EXTERNAL_LINKS,
    PAGES_WITH_LINKS,
    LINKS_FOUND,
    LINKS_USED,
    EXCEPTIONS,
  }

  protected static class ServerMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outVal = new LongWritable(1);

    @Override
    public void map(Text key, ArchiveReader value, Context context) throws IOException {
      for (ArchiveRecord r : value) {

        try {
          context.getCounter(MAPCOUNTER.PAGES).increment(1);
          if (!Link.isValid(r.getHeader().getUrl())) {
            context.getCounter(MAPCOUNTER.PAGES_BAD_URL).increment(1);
            continue;
          }
          Page page = Page.from(r);

          int totalLinks = page.getNumLinks();
          if (totalLinks == 0) {
            context.getCounter(MAPCOUNTER.PAGES_NO_LINKS).increment(1);
            continue;
          }
          context.getCounter(MAPCOUNTER.LINKS_FOUND).increment(page.getNumLinks());

          Set<Link> links = page.getExternalLinks();
          if (links.size() == 0) {
            context.getCounter(MAPCOUNTER.PAGES_NO_EXTERNAL_LINKS).increment(1);
            continue;
          }
          context.getCounter(MAPCOUNTER.PAGES_WITH_LINKS).increment(1);
          context.getCounter(MAPCOUNTER.LINKS_USED).increment(links.size());

          for (Link link : links) {
            outKey.set("u:" + link.getUri() + "\tcount");
            context.write(outKey, outVal);
            outKey.set("u:" + link.getUri() + "\tl:" + page.getLink().getUri()
                       + "\t" + link.getAnchorText());
            context.write(outKey, outVal);
            outKey.set("d:" + link.getReverseTopPrivate() + "\tu:" + link.getUri());
            context.write(outKey, outVal);
          }
        } catch (Exception e) {
          LOG.error("Caught Exception", e);
          context.getCounter(MAPCOUNTER.EXCEPTIONS).increment(1);
        }
      }
    }
  }
}
