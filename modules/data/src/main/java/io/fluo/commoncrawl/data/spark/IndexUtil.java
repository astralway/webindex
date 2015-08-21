package io.fluo.commoncrawl.data.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.core.models.Page;
import io.fluo.commoncrawl.data.util.ArchiveUtil;
import io.fluo.commoncrawl.data.util.LinkUtil;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import scala.Tuple2;

public class IndexUtil {

  private static Gson gson = new Gson();

  public static JavaPairRDD<String, Long> createLinkCounts(IndexStats stats,
                                                           JavaPairRDD<Text, ArchiveReader> archives) {

    JavaRDD<ArchiveRecord> records = archives.flatMap(
        new FlatMapFunction<Tuple2<Text, ArchiveReader>, ArchiveRecord>() {
          @Override
          public Iterable<ArchiveRecord> call(Tuple2<Text, ArchiveReader> tuple) throws Exception {
            return tuple._2();
          }
        });

    JavaRDD<Page> pages = records.map(r -> ArchiveUtil.buildPageIgnoreErrors(r));

    JavaRDD<String> links = pages.flatMap(
        new FlatMapFunction<Page, String>() {
          @Override
          public Iterable<String> call(Page page) throws Exception {
            if (page.isEmpty()) {
              stats.addEmpty(1);
              return new ArrayList<>();
            }
            stats.addPage(1);
            Set<Page.Link> links = page.getOutboundLinks();
            stats.addExternalLinks(links.size());

            List<String> retval = new ArrayList<>();
            String pageUri = page.getUri();
            String pageDomain = LinkUtil.getReverseTopPrivate(page.getUrl());
            if (links.size() > 0) {
              retval.add(String.format("p:%s\t%s:%s", pageUri, ColumnConstants.PAGE,
                                       ColumnConstants.SCORE));
              retval.add(String.format("p:%s\t%s:%s\t%s", pageUri, ColumnConstants.PAGE,
                                       ColumnConstants.CUR, gson.toJson(page)));
              retval.add(String.format("d:%s\t%s:%s", pageDomain, ColumnConstants.PAGES, pageUri));
            }
            for (Page.Link link : links) {
              String linkUri = link.getUri();
              String linkDomain = LinkUtil.getReverseTopPrivate(link.getUrl());
              retval.add(String.format("p:%s\t%s:%s", linkUri, ColumnConstants.PAGE,
                                       ColumnConstants.INCOUNT));
              retval.add(String.format("p:%s\t%s:%s", linkUri, ColumnConstants.PAGE,
                                       ColumnConstants.SCORE));
              retval.add(String.format("p:%s\t%s:%s\t%s", linkUri, ColumnConstants.INLINKS, pageUri,
                                       link.getAnchorText()));
              retval.add(String.format("d:%s\t%s:%s", linkDomain, ColumnConstants.PAGES, linkUri));
            }
            return retval;
          }
        });
    links.persist(StorageLevel.DISK_ONLY());

    final Long one = new Long(1);
    JavaPairRDD<String, Long> ones = links.mapToPair(s -> new Tuple2<>(s, one));

    JavaPairRDD<String, Long> linkCounts = ones.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Long> sortedLinkCounts = linkCounts.sortByKey();

    return sortedLinkCounts;
  }

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }

  public static JavaPairRDD<String, Long> createSortedTopCounts(JavaPairRDD<String, Long> sortedLinkCounts) {
    final Long one = new Long(1);
    JavaPairRDD<String, Long> topCounts = sortedLinkCounts.flatMapToPair(
        new PairFlatMapFunction<Tuple2<String, Long>, String, Long>() {
          @Override
          public Iterable<Tuple2<String, Long>> call(Tuple2<String, Long> t)
              throws Exception {
            List<Tuple2<String, Long>> retval = new ArrayList<>();
            String[] args = t._1().split("\t", 2);
            if (args[0].startsWith("d:") && (args[1].startsWith(ColumnConstants.PAGES))) {
              String domain = args[0];
              String link = args[1].substring(ColumnConstants.PAGES.length() + 1);
              Long numLinks = t._2();
              retval.add(new Tuple2<>(String.format("%s\t%s:%s:%s", domain, ColumnConstants.RANK,
                                                    revEncodeLong(numLinks), link), numLinks));
              retval.add(new Tuple2<>(String.format("%s\t%s:%s", domain, ColumnConstants.DOMAIN,
                                                    ColumnConstants.PAGECOUNT), one));
            } else if (args[1].startsWith(ColumnConstants.PAGE)) {
              String[] colArgs = args[1].split(":");
              if (colArgs[1].equals(ColumnConstants.INCOUNT) || colArgs[1].equals(ColumnConstants.SCORE)) {
                String page = args[0].substring(2);
                Long num = t._2();
                retval.add(new Tuple2<>(String.format("t:%s\t%s:%s:%s", colArgs[1], ColumnConstants.RANK,
                                                      revEncodeLong(num), page), num));
              }
              retval.add(t);
            } else {
              retval.add(t);
            }
            return retval;
          }
        });

    JavaPairRDD<String, Long> reducedTopCounts = topCounts.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Long> sortedTopCounts = reducedTopCounts.sortByKey();

    return sortedTopCounts;
  }
}
