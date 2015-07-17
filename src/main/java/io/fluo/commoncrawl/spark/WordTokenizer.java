package io.fluo.commoncrawl.spark;

import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordTokenizer implements Iterable<String> {

  private static final Logger log = LoggerFactory.getLogger(WordCount.class);
  private Iterator<ArchiveRecord> iter;
  private StringTokenizer tokenizer;

  WordTokenizer(ArchiveReader reader) {
    this.iter = reader.iterator();
    this.tokenizer = null;
  }

  @Override public Iterator<String> iterator() {

    return new Iterator<String>() {
      @Override public boolean hasNext() {
        return iter.hasNext() || ((tokenizer != null) && tokenizer.hasMoreTokens());
      }

      @Override public String next() {
        while (true) {
          if ((tokenizer == null) || !tokenizer.hasMoreTokens()) {
            if (!iter.hasNext()) {
              return null;
            }
            ArchiveRecord record = iter.next();
            try {
              if (record.getHeader().getMimetype().equals("text/plain")) {
                log.debug(record.getHeader().getUrl() + " -- " + record.available());
                byte[] rawData = IOUtils.toByteArray(record, record.available());
                String content = new String(rawData);
                tokenizer = new StringTokenizer(content, " \t\n\r\f,.:;?![]'");
              }
            } catch (Exception ex) {
              log.error("Caught Exception", ex);
            }
          }

          if ((tokenizer != null) && tokenizer.hasMoreTokens()) {
            return tokenizer.nextToken();
          }
        }
      }

      @Override public void remove() {
        // do nothing
      }
    };
  }
}
