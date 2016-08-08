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

package webindex.data.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.recipes.core.map.CollisionFreeMap;
import org.apache.fluo.recipes.core.map.CollisionFreeMap.Initializer;
import org.apache.fluo.recipes.kryo.KryoSimplerSerializer;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import scala.Tuple2;
import webindex.core.Constants;
import webindex.core.models.Link;
import webindex.core.models.Page;
import webindex.core.models.URL;
import webindex.data.fluo.DomainMap;
import webindex.data.fluo.PageObserver;
import webindex.data.fluo.UriCountExport;
import webindex.data.fluo.UriMap;
import webindex.data.fluo.UriMap.UriInfo;
import webindex.data.util.ArchiveUtil;
import webindex.data.util.FluoConstants;
import webindex.serialization.WebindexKryoFactory;

public class IndexUtil {

  private static Gson gson = new Gson();

  private static void addRCV(List<Tuple2<RowColumn, Bytes>> tuples, String r, Column c, Long v) {
    addRCV(tuples, r, c, v.toString());
  }

  private static void addRCV(List<Tuple2<RowColumn, Bytes>> tuples, String r, Column c, String v) {
    tuples.add(new Tuple2<>(new RowColumn(r, c), Bytes.of(v)));
  }

  /**
   * Creates an RDD of pages from an RDD archive
   */
  public static JavaRDD<Page> createPages(JavaPairRDD<Text, ArchiveReader> archives) {
    int numPartitions = 50 * (int) archives.count();
    JavaRDD<ArchiveRecord> records = archives.flatMap(Tuple2::_2);
    return records.map(ArchiveUtil::buildPageIgnoreErrors).repartition(numPartitions)
        .persist(StorageLevel.DISK_ONLY_2());
  }

  public static JavaPairRDD<String, UriInfo> createUriMap(JavaRDD<Page> pages) {
    JavaPairRDD<String, UriInfo> uriMap = pages.flatMapToPair(page -> {
      List<Tuple2<String, UriInfo>> ret = new ArrayList<>();

      if (!page.isEmpty()) {
        ret.add(new Tuple2<>(page.getPageID(), new UriInfo(0, 1)));

        for (Link link : page.getOutboundLinks()) {
          ret.add(new Tuple2<>(link.getPageID(), new UriInfo(1, 0)));
        }
      }
      return ret;
    }).reduceByKey(UriInfo::merge);

    uriMap.persist(StorageLevel.DISK_ONLY());

    return uriMap;
  }

  public static JavaPairRDD<String, Long> createDomainMap(JavaPairRDD<String, UriInfo> uriMap) {

    JavaPairRDD<String, Long> domainMap =
        uriMap.mapToPair(t -> new Tuple2<>(URL.fromPageID(t._1()).getReverseDomain(), 1L))
            .reduceByKey(Long::sum);

    domainMap.persist(StorageLevel.DISK_ONLY());

    return domainMap;
  }

  /**
   * Creates initial data for external Accumulo index table
   */
  public static JavaPairRDD<RowColumn, Bytes> createAccumuloIndex(IndexStats stats,
      JavaRDD<Page> pages, JavaPairRDD<String, UriInfo> uriMap, JavaPairRDD<String, Long> domainMap) {

    JavaPairRDD<RowColumn, Bytes> accumuloIndex =
        pages.flatMapToPair(page -> {
          if (page.isEmpty()) {
            stats.addEmpty(1);
            return new ArrayList<>();
          }
          stats.addPage(1);
          Set<Link> links1 = page.getOutboundLinks();
          stats.addExternalLinks(links1.size());

          List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
          String pageID = page.getPageID();
          if (links1.size() > 0) {
            addRCV(ret, "p:" + pageID, FluoConstants.PAGE_CUR_COL, gson.toJson(page));
          }
          for (Link link : links1) {
            addRCV(ret, "p:" + link.getPageID(), new Column(Constants.INLINKS, pageID),
                link.getAnchorText());
          }
          return ret;
        });

    accumuloIndex =
        accumuloIndex.union(uriMap.flatMapToPair(t -> {
          List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
          String uri = t._1();
          UriInfo uriInfo = t._2();
          addRCV(ret, "t:" + UriCountExport.revEncodeLong(uriInfo.linksTo) + ":" + uri,
              Column.EMPTY, uriInfo.linksTo);
          String domain = URL.fromPageID(t._1()).getReverseDomain();
          String domainRow = UriCountExport.encodeDomainRankPageId(domain, uriInfo.linksTo, uri);
          addRCV(ret, domainRow, new Column(Constants.RANK, ""), uriInfo.linksTo);
          addRCV(ret, "p:" + uri, FluoConstants.PAGE_INCOUNT_COL, uriInfo.linksTo);
          return ret;
        }));

    accumuloIndex =
        accumuloIndex.union(domainMap.mapToPair(t -> new Tuple2<>(new RowColumn("d:" + t._1(),
            new Column(Constants.DOMAIN, Constants.PAGECOUNT)), Bytes.of(t._2() + ""))));

    accumuloIndex.persist(StorageLevel.DISK_ONLY());

    return accumuloIndex;
  }

  /**
   * Creates initial data for Fluo table
   */
  public static JavaPairRDD<RowColumn, Bytes> createFluoTable(JavaRDD<Page> pages,
      JavaPairRDD<String, UriInfo> uriMap, JavaPairRDD<String, Long> domainMap, int numBuckets) {

    KryoSimplerSerializer serializer = new KryoSimplerSerializer(new WebindexKryoFactory());

    JavaPairRDD<RowColumn, Bytes> fluoIndex = pages.flatMapToPair(page -> {
      if (page.isEmpty()) {

        return new ArrayList<>();
      }
      Set<Link> links1 = page.getOutboundLinks();
      List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
      String pageID = page.getPageID();
      if (links1.size() > 0) {
        String hashedRow = PageObserver.getPageRowHasher().addHash(pageID).toString();
        addRCV(ret, hashedRow, new Column(Constants.PAGE, Constants.CUR), gson.toJson(page));
      }
      return ret;
    });

    Initializer<String, UriInfo> uriMapInitializer =
        CollisionFreeMap.getInitializer(UriMap.URI_MAP_ID, numBuckets, serializer);

    fluoIndex = fluoIndex.union(uriMap.mapToPair(t -> {
      RowColumnValue rcv = uriMapInitializer.convert(t._1(), t._2());
      return new Tuple2<>(new RowColumn(rcv.getRow(), rcv.getColumn()), rcv.getValue());
    }));

    Initializer<String, Long> domainMapInitializer =
        CollisionFreeMap.getInitializer(DomainMap.DOMAIN_MAP_ID, numBuckets, serializer);

    fluoIndex = fluoIndex.union(domainMap.mapToPair(t -> {
      RowColumnValue rcv = domainMapInitializer.convert(t._1(), t._2());
      return new Tuple2<>(new RowColumn(rcv.getRow(), rcv.getColumn()), rcv.getValue());
    }));

    fluoIndex.persist(StorageLevel.DISK_ONLY());

    return fluoIndex;
  }

  public static SortedSet<Text> calculateSplits(JavaPairRDD<RowColumn, Bytes> accumuloIndex,
      int numSplits) {
    List<Tuple2<RowColumn, Bytes>> sample = accumuloIndex.takeSample(false, numSplits);

    SortedSet<Text> splits = new TreeSet<>();
    for (Tuple2<RowColumn, Bytes> tuple : sample) {
      Bytes row = tuple._1().getRow();
      if (row.length() < 29) {
        splits.add(new Text(row.toArray()));
      } else {
        splits.add(new Text(row.subSequence(0, 29).toArray()));
      }
    }
    return splits;
  }

}
