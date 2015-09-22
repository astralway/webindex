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
import org.apache.spark.api.java.function.PairFunction;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import scala.Tuple2;

public class IndexUtil {

  private static Gson gson = new Gson();

  private static void addRCV(List<Tuple2<RowColumn, Long>> tuples, String r, Column c, Long v) {
    tuples.add(new Tuple2<>(new RowColumn(r, c), v));
  }

  public static JavaRDD<Page> createPages(JavaPairRDD<Text, ArchiveReader> archives) {

    JavaRDD<ArchiveRecord> records =
        archives.flatMap(new FlatMapFunction<Tuple2<Text, ArchiveReader>, ArchiveRecord>() {
          @Override
          public Iterable<ArchiveRecord> call(Tuple2<Text, ArchiveReader> tuple) throws Exception {
            return tuple._2();
          }
        });

    JavaRDD<Page> pages = records.map(r -> ArchiveUtil.buildPageIgnoreErrors(r));
    return pages;
  }

  public static JavaPairRDD<RowColumn, Bytes> createLinkIndex(IndexStats stats, JavaRDD<Page> pages) {

    JavaPairRDD<RowColumn, Long> links =
        pages.flatMapToPair(new PairFlatMapFunction<Page, RowColumn, Long>() {
          @Override
          public Iterable<Tuple2<RowColumn, Long>> call(Page page) throws Exception {
            if (page.isEmpty()) {
              stats.addEmpty(1);
              return new ArrayList<>();
            }
            stats.addPage(1);
            Set<Page.Link> links = page.getOutboundLinks();
            stats.addExternalLinks(links.size());

            List<Tuple2<RowColumn, Long>> ret = new ArrayList<>();
            String pageUri = page.getUri();
            String pageDomain = LinkUtil.getReverseTopPrivate(page.getUrl());
            final Long one = new Long(1);
            if (links.size() > 0) {
              addRCV(ret, "p:" + pageUri, FluoConstants.PAGE_INCOUNT_COL, new Long(0));
              addRCV(ret, "p:" + pageUri,
                  new Column(Constants.PAGE, Constants.CUR, gson.toJson(page)), one);
              addRCV(ret, "d:" + pageDomain, new Column(Constants.PAGES, pageUri), new Long(0));
            }
            for (Page.Link link : links) {
              String linkUri = link.getUri();
              String linkDomain = LinkUtil.getReverseTopPrivate(link.getUrl());
              addRCV(ret, "p:" + linkUri, FluoConstants.PAGE_INCOUNT_COL, one);
              addRCV(ret, "p:" + linkUri,
                  new Column(Constants.INLINKS, pageUri, link.getAnchorText()), one);
              addRCV(ret, "d:" + linkDomain, new Column(Constants.PAGES, linkUri), one);
            }
            return ret;
          }
        });

    JavaPairRDD<RowColumn, Long> linkCounts = links.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedLinkCounts = linkCounts.sortByKey();

    final Long one = new Long(1);
    JavaPairRDD<RowColumn, Long> topCounts =
        sortedLinkCounts
            .flatMapToPair(new PairFlatMapFunction<Tuple2<RowColumn, Long>, RowColumn, Long>() {
              @Override
              public Iterable<Tuple2<RowColumn, Long>> call(Tuple2<RowColumn, Long> t)
                  throws Exception {
                List<Tuple2<RowColumn, Long>> ret = new ArrayList<>();
                ret.add(t);

                RowColumn rc = t._1();
                String row = rc.getRow().toString();
                String cf = rc.getColumn().getFamily().toString();
                String cq = rc.getColumn().getQualifier().toString();
                Long val = t._2();

                if (row.startsWith("d:") && (cf.equals(Constants.PAGES))) {
                  addRCV(ret, row,
                      new Column(Constants.RANK, String.format("%s:%s", revEncodeLong(val), cq)),
                      val);
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
              }
            });

    JavaPairRDD<RowColumn, Long> reducedTopCounts = topCounts.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<RowColumn, Long> sortedTopCounts = reducedTopCounts.sortByKey();

    JavaPairRDD<RowColumn, Long> filteredTopCounts =
        sortedTopCounts.filter(t -> !(t._1().getRow().toString().startsWith("d:") && t._1()
            .getColumn().getFamily().toString().equals(Constants.PAGES)));

    JavaPairRDD<RowColumn, Bytes> linkIndex =
        filteredTopCounts.mapToPair(new PairFunction<Tuple2<RowColumn, Long>, RowColumn, Bytes>() {
          @Override
          public Tuple2<RowColumn, Bytes> call(Tuple2<RowColumn, Long> tuple) throws Exception {
            RowColumn rc = tuple._1();
            String cf = rc.getColumn().getFamily().toString();
            String cq = rc.getColumn().getQualifier().toString();
            byte[] val = tuple._2().toString().getBytes();
            if (cf.equals(Constants.INLINKS)
                || (cf.equals(Constants.PAGE) && cq.startsWith(Constants.CUR))) {
              val = rc.getColumn().getVisibility().toArray();
            }
            return new Tuple2<>(new RowColumn(rc.getRow(), new Column(cf, cq)), Bytes.of(val));
          }
        });

    return linkIndex;
  }

  public static JavaPairRDD<RowColumn, Bytes> filterRank(JavaPairRDD<RowColumn, Bytes> linkIndex) {
    JavaPairRDD<RowColumn, Bytes> filteredLinkIndex =
        linkIndex.filter(t -> !t._1().getColumn().getFamily().toString().equals(Constants.RANK));
    return filteredLinkIndex;
  }

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }
}
