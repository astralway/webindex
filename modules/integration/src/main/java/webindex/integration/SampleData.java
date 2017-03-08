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

package webindex.integration;

import java.io.BufferedWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.gson.Gson;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.models.Page;
import webindex.data.util.ArchiveUtil;

public class SampleData {

  private static final Logger log = LoggerFactory.getLogger(SampleData.class);

  private static final String sourceURL = "https://commoncrawl.s3.amazonaws.com/crawl-data/"
      + "CC-MAIN-2015-32/segments/1438042981460.12/wat/"
      + "CC-MAIN-20150728002301-00043-ip-10-236-191-2.ec2.internal.warc.wat.gz";

  public static void generate(Path path, int numPages) throws Exception {

    Gson gson = new Gson();
    long count = 0;
    try (BufferedWriter writer = Files.newBufferedWriter(path)) {
      ArchiveReader ar = WARCReaderFactory.get(new URL(sourceURL), 0);
      for (ArchiveRecord r : ar) {
        Page p = ArchiveUtil.buildPage(r);
        if (p.isEmpty() || p.getOutboundLinks().isEmpty()) {
          log.debug("Skipping {}", p.getUrl());
          continue;
        }
        log.debug("Found {} {}", p.getUrl(), p.getNumOutbound());
        String json = gson.toJson(p);
        writer.write(json);
        writer.newLine();
        count++;
        if (count == numPages) {
          break;
        } else if ((count % 1000) == 0) {
          log.info("Wrote {} of {} pages to {}", count, numPages, path);
        }
      }
    }
    log.info("Wrote {} pages to {}", numPages, path);
  }
}
