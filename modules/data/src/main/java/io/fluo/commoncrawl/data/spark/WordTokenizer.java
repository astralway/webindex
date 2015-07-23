package io.fluo.commoncrawl.data.spark;

import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordTokenizer implements Iterable<String> {

  private static final Logger log = LoggerFactory.getLogger(WordCount.class);

  private StringTokenizer tokenizer;

  WordTokenizer(ArchiveRecord record) {
    this.tokenizer = null;
    try {
      if (record.getHeader().getMimetype().equals("text/plain")) {
        byte[] rawData = IOUtils.toByteArray(record, record.available());
        String content = new String(rawData);
        tokenizer = new StringTokenizer(content, " \t\n\r\f,.:;?![]'");
      }
    } catch (Exception ex) {
      log.error("Caught Exception", ex);
    }
  }

  @Override public Iterator<String> iterator() {

    return new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return tokenizer != null && tokenizer.hasMoreTokens();
      }

      @Override
      public String next() {
        if ((tokenizer != null) && tokenizer.hasMoreTokens()) {
          return tokenizer.nextToken();
        }
        return null;
      }

      @Override
      public void remove() {
        // do nothing
      }
    };
  }
}
