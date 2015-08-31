package io.fluo.commoncrawl.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.commoncrawl.core.Constants;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.core.DataUtil;
import io.fluo.commoncrawl.data.spark.IndexEnv;
import io.fluo.commoncrawl.data.spark.IndexUtil;
import io.fluo.commoncrawl.data.util.LinkUtil;
import io.fluo.mapreduce.FluoEntryInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Reindex {

  private static final Logger log = LoggerFactory.getLogger(Reindex.class);

  private static IndexEnv env;

  static class RowColumnComparator implements Comparator<RowColumn>, Serializable {
    final static RowColumnComparator INSTANCE = new RowColumnComparator();

    @Override
    public int compare(RowColumn o1, RowColumn o2) {
      int result;
      result = o1.getRow().toString().compareTo(o2.getRow().toString());
      if (result == 0) {
        Column c1 = o1.getColumn();
        Column c2 = o2.getColumn();
        result = c1.getFamily().toString().compareTo(c2.getFamily().toString());
        if (result == 0) {
          result = c1.getQualifier().toString().compareTo(c2.getQualifier().toString());
          if (result == 0) {
            result = c1.getVisibility().toString().compareTo(c2.getVisibility().toString());
          }
        }
      }
      return result;
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Reindex <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);

    try {
      SparkConf sparkConf = new SparkConf().setAppName("CC-Reindex");
      env = new IndexEnv(dataConfig, sparkConf);
      env.initAccumuloIndexTable();
      env.makeHdfsTempDirs();
    } catch (Exception e) {
      log.error("Env setup failed due to exception", e);
      System.exit(-1);
    }

    Job job = Job.getInstance(env.getSparkCtx().hadoopConfiguration());
    FluoEntryInputFormat.configure(job, env.getFluoConfig());

    JavaPairRDD<RowColumn, Bytes> fluoData =
        env.getSparkCtx().newAPIHadoopRDD(job.getConfiguration(), FluoEntryInputFormat.class,
            RowColumn.class, Bytes.class);

    JavaPairRDD<RowColumn, Bytes> indexData =
        fluoData
            .flatMapToPair(new PairFlatMapFunction<Tuple2<RowColumn, Bytes>, RowColumn, Bytes>() {
              @Override
              public Iterable<Tuple2<RowColumn, Bytes>> call(Tuple2<RowColumn, Bytes> kvTuple)
                  throws Exception {

                List<Tuple2<RowColumn, Bytes>> retval = new ArrayList<>();
                retval.add(kvTuple);
                RowColumn rc = kvTuple._1();
                String row = rc.getRow().toString();
                String cf = rc.getColumn().getFamily().toString();
                String cq = rc.getColumn().getQualifier().toString();
                Bytes v = kvTuple._2();
                if (row.startsWith("p:") && cf.equals(Constants.PAGE)
                    && (cq.equals(Constants.INCOUNT) || cq.equals(Constants.SCORE))) {
                  String pageUri = row.substring(2);
                  Long num = Long.parseLong(v.toString());
                  Column rankCol =
                      new Column(Constants.RANK, String.format("%s:%s",
                          IndexUtil.revEncodeLong(num), pageUri));
                  if (cq.equals(Constants.SCORE)) {
                    String domain = "d:" + LinkUtil.getReverseTopPrivate(DataUtil.toUrl(pageUri));
                    retval.add(new Tuple2<>(new RowColumn(domain, rankCol), v));
                  }
                  retval.add(new Tuple2<>(new RowColumn("t:" + cq, rankCol), v));
                }
                return retval;
              }
            });

    JavaPairRDD<RowColumn, Bytes> sortedIndexes = indexData.sortByKey(RowColumnComparator.INSTANCE);

    env.saveRowColBytesToAccumulo(sortedIndexes);

  }
}
