package io.fluo.commoncrawl.inbound;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class LinkMap {

  private static final Logger LOG = Logger.getLogger(LinkMap.class);

  protected static enum MAPCOUNTER {
    PAGES,
    PAGES_BAD_URI,
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
          Page page = new Page(r);
          String srcUri;
          try {
            srcUri = page.getUri();
          } catch (Exception e) {
            context.getCounter(MAPCOUNTER.PAGES_BAD_URI).increment(1);
            continue;
          }

          int totalLinks = page.getNumLinks();
          if (totalLinks == 0) {
            context.getCounter(MAPCOUNTER.PAGES_NO_LINKS).increment(1);
            continue;
          }
          context.getCounter(MAPCOUNTER.LINKS_FOUND).increment(page.getNumLinks());

          Map<String, String> links = page.getExternalUriLinks();
          if (links.size() == 0) {
            context.getCounter(MAPCOUNTER.PAGES_NO_EXTERNAL_LINKS).increment(1);
            continue;
          }
          context.getCounter(MAPCOUNTER.PAGES_WITH_LINKS).increment(1);
          context.getCounter(MAPCOUNTER.LINKS_USED).increment(links.size());

          for (Map.Entry<String, String> entry : links.entrySet()) {
            String uri = entry.getKey();
            String domain = LinkUtil.getDomainFromUri(uri);
            String text = entry.getValue();
            outKey.set("u:" + uri + "\tcount");
            context.write(outKey, outVal);
            outKey.set("u:" + uri + "\tl:" + srcUri + "\t" + text);
            context.write(outKey, outVal);
            outKey.set("u:" + domain + "\tcount");
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
