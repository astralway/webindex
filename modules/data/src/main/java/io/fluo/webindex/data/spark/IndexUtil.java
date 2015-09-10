package io.fluo.webindex.data.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.util.ArchiveUtil;
import io.fluo.webindex.data.util.FluoConstants;
import io.fluo.webindex.data.util.LinkUtil;
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

  public static JavaPairRDD<RowColumn, Long> createLinkCounts(IndexStats stats,
      JavaPairRDD<Text, ArchiveReader> archives) {

    JavaRDD<ArchiveRecord> records =
        archives.flatMap(new FlatMapFunction<Tuple2<Text, ArchiveReader>, ArchiveRecord>() {
          @Override
          public Iterable<ArchiveRecord> call(Tuple2<Text, ArchiveReader> tuple) throws Exception {
            return tuple._2();
          }
        });

    JavaRDD<Page> pages = records.map(r -> ArchiveUtil.buildPageIgnoreErrors(r));

    JavaRDD<RowColumn> links = pages.flatMap(new FlatMapFunction<Page, RowColumn>() {
      @Override
      public Iterable<RowColumn> call(Page page) throws Exception {
        if (page.isEmpty()) {
          stats.addEmpty(1);
          return new ArrayList<>();
        }
        stats.addPage(1);
        Set<Page.Link> links = page.getOutboundLinks();
        stats.addExternalLinks(links.size());

        List<RowColumn> retval = new ArrayList<>();
        String pageUri = page.getUri();
        String pageDomain = LinkUtil.getReverseTopPrivate(page.getUrl());
        if (links.size() > 0) {
          retval.add(new RowColumn("p:" + pageUri, FluoConstants.PAGE_SCORE_COL));
          retval.add(new RowColumn("p:" + pageUri, new Column(Constants.PAGE, Constants.CUR, gson
              .toJson(page))));
          retval.add(new RowColumn("d:" + pageDomain, new Column(Constants.PAGES, pageUri)));
        }
        for (Page.Link link : links) {
          String linkUri = link.getUri();
          String linkDomain = LinkUtil.getReverseTopPrivate(link.getUrl());
          retval.add(new RowColumn("p:" + linkUri, FluoConstants.PAGE_INCOUNT_COL));
          retval.add(new RowColumn("p:" + linkUri, FluoConstants.PAGE_SCORE_COL));
          retval.add(new RowColumn("p:" + linkUri, new Column(Constants.INLINKS, pageUri, link
              .getAnchorText())));
          retval.add(new RowColumn("d:" + linkDomain, new Column(Constants.PAGES, linkUri)));
        }
        return retval;
      }
    });
    links.persist(StorageLevel.DISK_ONLY());

    final Long one = new Long(1);
    JavaPairRDD<RowColumn, Long> ones = links.mapToPair(s -> new Tuple2<>(s, one));

    JavaPairRDD<RowColumn, Long> linkCounts = ones.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedLinkCounts = linkCounts.sortByKey();

    return sortedLinkCounts;
  }

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }

  public static JavaPairRDD<RowColumn, Long> createSortedTopCounts(
      JavaPairRDD<RowColumn, Long> sortedLinkCounts) {
    final Long one = new Long(1);
    JavaPairRDD<RowColumn, Long> topCounts =
        sortedLinkCounts
            .flatMapToPair(new PairFlatMapFunction<Tuple2<RowColumn, Long>, RowColumn, Long>() {
              @Override
              public Iterable<Tuple2<RowColumn, Long>> call(Tuple2<RowColumn, Long> t)
                  throws Exception {
                List<Tuple2<RowColumn, Long>> retval = new ArrayList<>();
                retval.add(t);

                RowColumn rc = t._1();
                String row = rc.getRow().toString();
                String cf = rc.getColumn().getFamily().toString();
                String cq = rc.getColumn().getQualifier().toString();
                Long val = t._2();

                if (row.startsWith("d:") && (cf.equals(Constants.PAGES))) {
                  retval.add(new Tuple2<>(new RowColumn(row, new Column(Constants.RANK, String
                      .format("%s:%s", revEncodeLong(val), cq))), val));
                  retval.add(new Tuple2<>(new RowColumn(row, new Column(Constants.DOMAIN,
                      Constants.PAGECOUNT)), one));
                } else if ((row.startsWith("p:") && cf.equals(Constants.PAGE))
                    && (cq.equals(Constants.INCOUNT) || cq.equals(Constants.SCORE))) {
                  retval.add(new Tuple2<>(new RowColumn("t:" + cq, new Column(Constants.RANK,
                      String.format("%s:%s", revEncodeLong(val), row.substring(2)))), val));
                }
                return retval;
              }
            });

    JavaPairRDD<RowColumn, Long> reducedTopCounts = topCounts.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedTopCounts = reducedTopCounts.sortByKey();

    return sortedTopCounts;
  }
}
