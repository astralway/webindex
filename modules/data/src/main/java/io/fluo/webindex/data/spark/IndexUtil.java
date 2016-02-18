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
import io.fluo.webindex.core.models.Page.Link;
import io.fluo.webindex.data.fluo.DomainMap;
import io.fluo.webindex.data.fluo.PageObserver;
import io.fluo.webindex.data.fluo.UriCountExport;
import io.fluo.webindex.data.fluo.UriMap;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.util.ArchiveUtil;
import io.fluo.webindex.data.util.FluoConstants;
import io.fluo.webindex.data.util.LinkUtil;
import io.fluo.webindex.serialization.WebindexKryoFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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
        ret.add(new Tuple2<>(page.getUri(), new UriInfo(0, 1)));

        for (Link link : page.getOutboundLinks()) {
          ret.add(new Tuple2<>(link.getUri(), new UriInfo(1, 0)));
        }
      }
      return ret;
    }).reduceByKey(UriInfo::merge);

    uriMap.persist(StorageLevel.DISK_ONLY());

    return uriMap;
  }

  public static JavaPairRDD<String, Long> createDomainMap(JavaPairRDD<String, UriInfo> uriMap) {

    JavaPairRDD<String, Long> domainMap = uriMap.mapToPair(t -> {
      String domain = LinkUtil.getReverseTopPrivate(DataUtil.toUrl(t._1()));
      return new Tuple2<>(domain, 1l);
    }).reduceByKey(Long::sum);

    domainMap.persist(StorageLevel.DISK_ONLY());

    return domainMap;
  }

  /**
   * Creates initial data for external Accumulo index table
   */
  public static JavaPairRDD<RowColumn, Bytes> createAccumuloIndex(IndexStats stats,
      JavaRDD<Page> pages, JavaPairRDD<String, UriInfo> uriMap, JavaPairRDD<String, Long> domainMap) {

    JavaPairRDD<RowColumn, Bytes> accumuloIndex = pages.flatMapToPair(page -> {
      if (page.isEmpty()) {
        stats.addEmpty(1);
        return new ArrayList<>();
      }
      stats.addPage(1);
      Set<Page.Link> links1 = page.getOutboundLinks();
      stats.addExternalLinks(links1.size());

      List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
      String pageUri = page.getUri();
      if (links1.size() > 0) {
        addRCV(ret, "p:" + pageUri, new Column(Constants.PAGE, Constants.CUR), gson.toJson(page));
      }
      for (Page.Link link : links1) {
        String linkUri = link.getUri();
        addRCV(ret, "p:" + linkUri, new Column(Constants.INLINKS, pageUri), link.getAnchorText());
      }
      return ret;
    });

    accumuloIndex =
        accumuloIndex
            .union(uriMap.flatMapToPair(t -> {
              List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
              String uri = t._1();
              UriInfo uriInfo = t._2();
              addRCV(ret, "t:" + UriCountExport.revEncodeLong(uriInfo.linksTo) + ":" + uri,
                  Column.EMPTY, uriInfo.linksTo);
              String domain = LinkUtil.getReverseTopPrivate(DataUtil.toUrl(t._1()));
              addRCV(ret, "d:" + domain,
                  new Column(Constants.RANK, UriCountExport.revEncodeLong(uriInfo.linksTo) + ":"
                      + uri), uriInfo.linksTo);
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
      Set<Page.Link> links1 = page.getOutboundLinks();
      List<Tuple2<RowColumn, Bytes>> ret = new ArrayList<>();
      String pageUri = page.getUri();
      if (links1.size() > 0) {
        String hashedRow = PageObserver.getPageRowHasher().addHash(pageUri).toString();
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

  public static void saveRowColBytesToFluo(JavaPairRDD<RowColumn, Bytes> data,
      JavaSparkContext ctx, Connector conn, FluoConfiguration fluoConfig, Path fluoTempDir,
      Path failuresDir) throws Exception {

    // partition and sort data so that one file is created per an accumulo tablet
    data =
        data.repartitionAndSortWithinPartitions(new AccumuloRangePartitioner(conn.tableOperations()
            .listSplits(fluoConfig.getAccumuloTable())));

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

    // partition and sort data so that one file is created per an accumulo tablet
    data =
        data.repartitionAndSortWithinPartitions(new AccumuloRangePartitioner(conn.tableOperations()
            .listSplits(accumuloTable)));

    JavaPairRDD<Key, Value> kvData = data.mapToPair(tuple -> {
      RowColumn rc = tuple._1();
      byte[] row = rc.getRow().toArray();
      byte[] cf = rc.getColumn().getFamily().toArray();
      byte[] cq = rc.getColumn().getQualifier().toArray();
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

}
