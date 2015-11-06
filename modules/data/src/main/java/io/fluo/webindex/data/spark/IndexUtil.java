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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.api.data.RowColumnValue;
import io.fluo.mapreduce.FluoKeyValue;
import io.fluo.mapreduce.FluoKeyValueGenerator;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.recipes.map.CollisionFreeMap.Initializer;
import io.fluo.recipes.serialization.KryoSimplerSerializer;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataUtil;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.data.fluo.DomainMap;
import io.fluo.webindex.data.fluo.UriMap;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.util.ArchiveUtil;
import io.fluo.webindex.data.util.FluoConstants;
import io.fluo.webindex.data.util.LinkUtil;
import io.fluo.webindex.serialization.WebindexKryoFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class IndexUtil {

  private static final Logger log = LoggerFactory.getLogger(IndexUtil.class);

  private static Gson gson = new Gson();

  private static void addRCV(List<Tuple2<RowColumn, Long>> tuples, String r, Column c, Long v) {
    tuples.add(new Tuple2<>(new RowColumn(r, c), v));
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
    links.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Long> linkCounts = links.reduceByKey((i1, i2) -> i1 + i2);
    linkCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Long> sortedLinkCounts = linkCounts.sortByKey();
    sortedLinkCounts.persist(StorageLevel.DISK_ONLY());

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
            addRCV(ret, String.format("t:%s:%s", revEncodeLong(val), row.substring(2)),
                Column.EMPTY, val);
          }
          return ret;
        });
    topCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Long> reducedTopCounts = topCounts.reduceByKey((i1, i2) -> i1 + i2);
    reducedTopCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Long> sortedTopCounts = reducedTopCounts.sortByKey();
    sortedTopCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Long> filteredTopCounts =
        sortedTopCounts.filter(t -> !(t._1().getRow().toString().startsWith("d:") && t._1()
            .getColumn().getFamily().toString().equals(Constants.PAGES)));
    filteredTopCounts.persist(StorageLevel.DISK_ONLY());

    JavaPairRDD<RowColumn, Bytes> accumuloIndex =
        filteredTopCounts.mapToPair(tuple -> {
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
    accumuloIndex.persist(StorageLevel.DISK_ONLY());
    return accumuloIndex;
  }

  /**
   * Creates a Fluo index by filtering out unnecessary data from Accumulo Index
   */
  public static JavaPairRDD<RowColumn, Bytes> createFluoIndex(
      JavaPairRDD<RowColumn, Bytes> accumuloIndex, int numBuckets) {

    KryoSimplerSerializer serializer = new KryoSimplerSerializer(new WebindexKryoFactory());

    JavaPairRDD<RowColumn, Bytes> pagesFiltered =
        accumuloIndex.filter(t -> {
          RowColumn rc = t._1();
          String row = rc.getRow().toString();
          String cf = rc.getColumn().getFamily().toString();
          String cq = rc.getColumn().getQualifier().toString();

          return row.startsWith("p:") && cf.equals(Constants.PAGE)
              && (cq.equals(Constants.INCOUNT) || cq.equals(Constants.CUR));
        });

    JavaPairRDD<Bytes, Iterable<Tuple2<RowColumn, Bytes>>> pageRows =
        pagesFiltered.groupBy(t -> t._1().getRow());

    Initializer<String, UriInfo> uriMapInitializer =
        CollisionFreeMap.getInitializer(UriMap.URI_MAP_ID, numBuckets, serializer);

    JavaPairRDD<RowColumn, Bytes> uriMap =
        pageRows.mapToPair(t -> {
          int docs = 0;
          long links = 0;

          for (Tuple2<RowColumn, Bytes> rcv : t._2()) {
            String cq = rcv._1().getColumn().getQualifier().toString();

            if (cq.equals(Constants.CUR)) {
              docs = 1;
            } else if (cq.equals(Constants.INCOUNT)) {
              links = Long.parseLong(rcv._2().toString());
            }
          }

          String uri = t._1().toString().substring(2);

          RowColumnValue rcv = uriMapInitializer.convert(uri, new UriInfo(links, docs));

          return new Tuple2<RowColumn, Bytes>(new RowColumn(rcv.getRow(), rcv.getColumn()), rcv
              .getValue());
        });

    Initializer<String, Long> domainMapInitializer =
        CollisionFreeMap.getInitializer(DomainMap.DOMAIN_MAP_ID, numBuckets, serializer);

    // generate the rest of the fluo table
    JavaPairRDD<RowColumn, Bytes> fluoIndex =
        accumuloIndex.flatMapToPair(
            t -> {
              RowColumn rc = t._1();
              String row = rc.getRow().toString();
              String cf = rc.getColumn().getFamily().toString();
              String cq = rc.getColumn().getQualifier().toString();

              if (row.startsWith("d:") && cf.equals(Constants.DOMAIN)
                  && cq.equals(Constants.PAGECOUNT)) {
                String domain = row.substring(2);
                Long count = Long.valueOf(t._2().toString());

                RowColumnValue rcv = domainMapInitializer.convert(domain, count);

                return Collections.singleton(new Tuple2<RowColumn, Bytes>(new RowColumn(rcv
                    .getRow(), rcv.getColumn()), rcv.getValue()));
              }
              if (row.startsWith("p:") && cf.equals(Constants.PAGE) && cq.equals(Constants.CUR)) {
                return Collections.singleton(t);
              }

              return Collections.emptyList();
            }).union(uriMap);



    fluoIndex.persist(StorageLevel.DISK_ONLY());

    fluoIndex = fluoIndex.sortByKey();

    fluoIndex.persist(StorageLevel.DISK_ONLY());

    return fluoIndex;
  }

  /**
   * Creates an Accumulo index by expanding a Fluo index
   */
  public static JavaPairRDD<RowColumn, Bytes> createAccumuloIndex(
      JavaPairRDD<RowColumn, Bytes> fluoIndex) {
    JavaPairRDD<RowColumn, Bytes> indexWithRank = fluoIndex.flatMapToPair(kvTuple -> {
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
        String rank = String.format("%s:%s", IndexUtil.revEncodeLong(num), pageUri);
        String domain = "d:" + LinkUtil.getReverseTopPrivate(DataUtil.toUrl(pageUri));
        retval.add(new Tuple2<>(new RowColumn(domain, new Column(Constants.RANK, rank)), v));
        retval.add(new Tuple2<>(new RowColumn("t:" + rank, Column.EMPTY), v));
      }
      return retval;
    });

    return indexWithRank.sortByKey();
  }

  public static void saveRowColBytesToFluo(JavaPairRDD<RowColumn, Bytes> data,
      JavaSparkContext ctx, Connector conn, FluoConfiguration fluoConfig, Path fluoTempDir,
      Path failuresDir) throws Exception {
    JavaPairRDD<Key, Value> fluoData = data.flatMapToPair(tuple -> {
      List<Tuple2<Key, Value>> output = new LinkedList<>();
      RowColumn rc = tuple._1();
      FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
      fkvg.setRow(rc.getRow()).setColumn(rc.getColumn()).setValue(tuple._2().toArray());
      for (FluoKeyValue kv : fkvg.getKeyValues()) {
        output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
      }
      return output;
    });
    saveKeyValueToFluo(fluoData, ctx, conn, fluoConfig, fluoTempDir, failuresDir);
  }

  public static void saveKeyValueToFluo(JavaPairRDD<Key, Value> data, JavaSparkContext ctx,
      Connector conn, FluoConfiguration fluoConfig, Path fluoTempDir, Path failuresDir)
      throws Exception {
    Job job = Job.getInstance(ctx.hadoopConfiguration());

    FileSystem hdfs = FileSystem.get(ctx.hadoopConfiguration());
    if (hdfs.exists(fluoTempDir)) {
      hdfs.delete(fluoTempDir, true);
    }
    if (!hdfs.exists(failuresDir)) {
      hdfs.mkdirs(failuresDir);
    }
    AccumuloFileOutputFormat.setOutputPath(job, fluoTempDir);

    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(fluoTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, job.getConfiguration());
    conn.tableOperations().importDirectory(fluoConfig.getAccumuloTable(), fluoTempDir.toString(),
        failuresDir.toString(), false);
    log.info("Imported data at {} into Fluo table {}", fluoTempDir, fluoConfig.getApplicationName());
  }

  public static void saveRowColBytesToAccumulo(JavaPairRDD<RowColumn, Bytes> data,
      JavaSparkContext ctx, Connector conn, Path accumuloTempDir, Path failuresDir,
      String accumuloTable) throws Exception {
    JavaPairRDD<Key, Value> kvData = data.mapToPair(tuple -> {
      RowColumn rc = tuple._1();
      String row = rc.getRow().toString();
      String cf = rc.getColumn().getFamily().toString();
      String cq = rc.getColumn().getQualifier().toString();
      byte[] val = tuple._2().toArray();
      return new Tuple2<>(new Key(new Text(row), new Text(cf), new Text(cq), 0), new Value(val));
    });
    saveKeyValueToAccumulo(kvData, ctx, conn, accumuloTempDir, failuresDir, accumuloTable);
  }

  public static void saveKeyValueToAccumulo(JavaPairRDD<Key, Value> data, JavaSparkContext ctx,
      Connector conn, Path accumuloTempDir, Path failuresDir, String accumuloTable)
      throws Exception {
    Job accJob = Job.getInstance(ctx.hadoopConfiguration());

    FileSystem hdfs = FileSystem.get(ctx.hadoopConfiguration());
    if (hdfs.exists(accumuloTempDir)) {
      hdfs.delete(accumuloTempDir, true);
    }
    if (!hdfs.exists(failuresDir)) {
      hdfs.mkdirs(failuresDir);
    }
    AccumuloFileOutputFormat.setOutputPath(accJob, accumuloTempDir);
    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(accumuloTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, accJob.getConfiguration());
    conn.tableOperations().importDirectory(accumuloTable, accumuloTempDir.toString(),
        failuresDir.toString(), false);
    log.info("Imported data at {} into Accumulo table {}", accumuloTempDir, accumuloTable);
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

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }
}
