/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.webindex.data.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataUtil;
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
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import scala.Tuple2;

public class IndexUtil {

  private static Gson gson = new Gson();

  private static void addRCV(List<Tuple2<RowColumn, Long>> tuples, String r, Column c, Long v) {
    tuples.add(new Tuple2<>(new RowColumn(r, c), v));
  }

  /**
   * Creates an RDD of pages from an RDD archive
   */
  public static JavaRDD<Page> createPages(JavaPairRDD<Text, ArchiveReader> archives) {
    int numPartitions = 10 * (int) archives.count();
    JavaRDD<ArchiveRecord> records = archives.flatMap(Tuple2::_2);
    return records.map(ArchiveUtil::buildPageIgnoreErrors).repartition(numPartitions);
  }

  /**
   * Creates an Accumulo index from an RDD of web pages
   */
  public static JavaPairRDD<RowColumn, Bytes> createAccumuloIndex(IndexStats stats,
      JavaRDD<Page> pages) {

    JavaPairRDD<RowColumn, Long> links =
        pages.flatMapToPair(page -> {
          if (page.isEmpty()) {
            stats.addEmpty(1);
            return new ArrayList<>();
          }
          stats.addPage(1);
          Set<Page.Link> links1 = page.getOutboundLinks();
          stats.addExternalLinks(links1.size());

          List<Tuple2<RowColumn, Long>> ret = new ArrayList<>();
          String pageUri = page.getUri();
          String pageDomain = LinkUtil.getReverseTopPrivate(page.getUrl());
          final Long one = (long) 1;
          if (links1.size() > 0) {
            addRCV(ret, "p:" + pageUri, FluoConstants.PAGE_INCOUNT_COL, (long) 0);
            addRCV(ret, "p:" + pageUri,
                new Column(Constants.PAGE, Constants.CUR, gson.toJson(page)), one);
            addRCV(ret, "d:" + pageDomain, new Column(Constants.PAGES, pageUri), (long) 0);
          }
          for (Page.Link link : links1) {
            String linkUri = link.getUri();
            String linkDomain = LinkUtil.getReverseTopPrivate(link.getUrl());
            addRCV(ret, "p:" + linkUri, FluoConstants.PAGE_INCOUNT_COL, one);
            addRCV(ret, "p:" + linkUri,
                new Column(Constants.INLINKS, pageUri, link.getAnchorText()), one);
            addRCV(ret, "d:" + linkDomain, new Column(Constants.PAGES, linkUri), one);
          }
          return ret;
        });

    JavaPairRDD<RowColumn, Long> linkCounts = links.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedLinkCounts = linkCounts.sortByKey();

    final Long one = (long) 1;
    JavaPairRDD<RowColumn, Long> topCounts =
        sortedLinkCounts.flatMapToPair(t -> {
          List<Tuple2<RowColumn, Long>> ret = new ArrayList<>();
          ret.add(t);

          RowColumn rc = t._1();
          String row = rc.getRow().toString();
          String cf = rc.getColumn().getFamily().toString();
          String cq = rc.getColumn().getQualifier().toString();
          Long val = t._2();

          if (row.startsWith("d:") && (cf.equals(Constants.PAGES))) {
            addRCV(ret, row,
                new Column(Constants.RANK, String.format("%s:%s", revEncodeLong(val), cq)), val);
            addRCV(ret, row, new Column(Constants.DOMAIN, Constants.PAGECOUNT), one);
          } else if ((row.startsWith("p:") && cf.equals(Constants.PAGE))
              && (cq.equals(Constants.INCOUNT))) {
            addRCV(
                ret,
                "t:" + cq,
                new Column(Constants.RANK, String.format("%s:%s", revEncodeLong(val),
                    row.substring(2))), val);
          }
          return ret;
        });

    JavaPairRDD<RowColumn, Long> reducedTopCounts = topCounts.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedTopCounts = reducedTopCounts.sortByKey();

    JavaPairRDD<RowColumn, Long> filteredTopCounts =
        sortedTopCounts.filter(t -> !(t._1().getRow().toString().startsWith("d:") && t._1()
            .getColumn().getFamily().toString().equals(Constants.PAGES)));

    return filteredTopCounts.mapToPair(tuple -> {
      RowColumn rc = tuple._1();
      String cf = rc.getColumn().getFamily().toString();
      String cq = rc.getColumn().getQualifier().toString();
      byte[] val = tuple._2().toString().getBytes();
      if (cf.equals(Constants.INLINKS)
          || (cf.equals(Constants.PAGE) && cq.startsWith(Constants.CUR))) {
        val = rc.getColumn().getVisibility().toArray();
      }
      return new Tuple2<>(new RowColumn(rc.getRow(), new Column(cf, cq)), Bytes.of(val));
    });
  }

  /**
   * Creates a Fluo index by filtering out unnecessary data from Accumulo Index
   */
  public static JavaPairRDD<RowColumn, Bytes> createFluoIndex(
      JavaPairRDD<RowColumn, Bytes> accumuloIndex) {
    return accumuloIndex.filter(t -> !t._1().getColumn().getFamily().toString()
        .equals(Constants.RANK));
  }

  /**
   * Creates an Accumulo index by expanding a Fluo index
   */
  public static JavaPairRDD<RowColumn, Bytes> createAccumuloIndex(
      JavaPairRDD<RowColumn, Bytes> fluoIndex) {
    JavaPairRDD<RowColumn, Bytes> indexWithRank =
        fluoIndex.flatMapToPair(kvTuple -> {
          List<Tuple2<RowColumn, Bytes>> retval = new ArrayList<>();
          retval.add(kvTuple);
          RowColumn rc = kvTuple._1();
          String row = rc.getRow().toString();
          String cf = rc.getColumn().getFamily().toString();
          String cq = rc.getColumn().getQualifier().toString();
          Bytes v = kvTuple._2();
          if (row.startsWith("p:") && cf.equals(Constants.PAGE) && cq.equals(Constants.INCOUNT)) {
            String pageUri = row.substring(2);
            Long num = Long.parseLong(v.toString());
            Column rankCol =
                new Column(Constants.RANK, String.format("%s:%s", IndexUtil.revEncodeLong(num),
                    pageUri));
            String domain = "d:" + LinkUtil.getReverseTopPrivate(DataUtil.toUrl(pageUri));
            retval.add(new Tuple2<>(new RowColumn(domain, rankCol), v));
            retval.add(new Tuple2<>(new RowColumn("t:" + cq, rankCol), v));
          }
          return retval;
        });

    return indexWithRank.sortByKey();
  }

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }
}
