package io.fluo.commoncrawl.data.spark;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.core.util.SpanUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class IndexEnv {

  private static final Logger log = LoggerFactory.getLogger(IndexEnv.class);

  private JavaSparkContext sparkCtx;
  private DataConfig dataConfig;
  private Connector conn;
  private FluoConfiguration fluoConfig;
  private FileSystem hdfs;
  private Path failuresDir;
  private Path hadoopTempDir;
  private Path accumuloTempDir;
  private Path fluoTempDir;

  public IndexEnv(DataConfig dataConfig, SparkConf sparkConf) throws IOException {
    this.dataConfig = dataConfig;
    Preconditions.checkNotNull(dataConfig.fluoPropsPath);
    Preconditions.checkArgument(new File(dataConfig.fluoPropsPath).exists(),
        "fluoPropsPath must be set in data.yml and exist");
    fluoConfig = new FluoConfiguration(new File(dataConfig.fluoPropsPath));
    conn = AccumuloUtil.getConnector(fluoConfig);
    sparkCtx = new JavaSparkContext(sparkConf);
    hdfs = FileSystem.get(sparkCtx.hadoopConfiguration());
    hadoopTempDir = new Path(dataConfig.hdfsTempDir + "/hadoop");
    fluoTempDir = new Path(dataConfig.hdfsTempDir + "/fluo");
    failuresDir = new Path(dataConfig.hdfsTempDir + "/failures");
    accumuloTempDir = new Path(dataConfig.hdfsTempDir + "/accumulo");
  }

  public void initAccumuloIndexTable() throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException, TableExistsException {
    if (conn.tableOperations().exists(dataConfig.accumuloIndexTable)) {
      conn.tableOperations().delete(dataConfig.accumuloIndexTable);
    }
    conn.tableOperations().create(dataConfig.accumuloIndexTable);
  }

  public void makeHdfsTempDirs() throws IOException {
    Path tempDir = new Path(dataConfig.hdfsTempDir);
    if (!hdfs.exists(tempDir)) {
      hdfs.mkdirs(tempDir);
    }
    if (!hdfs.exists(failuresDir)) {
      hdfs.mkdirs(failuresDir);
    }
  }

  public void saveToFluo(JavaPairRDD<Key, Value> data) throws Exception {
    Job job = Job.getInstance(sparkCtx.hadoopConfiguration());

    if (hdfs.exists(fluoTempDir)) {
      hdfs.delete(fluoTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(job, fluoTempDir);

    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(fluoTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, job.getConfiguration());
    conn.tableOperations().importDirectory(fluoConfig.getAccumuloTable(), fluoTempDir.toString(),
        failuresDir.toString(), false);
    log.info("Imported data at {} into Fluo app {}", fluoTempDir, fluoConfig.getApplicationName());
  }

  public void saveRowColBytesToAccumulo(JavaPairRDD<RowColumn, Bytes> rowColBytes) throws Exception {
    JavaPairRDD<Key, Value> kvData =
        rowColBytes.mapToPair(t -> new Tuple2<Key, Value>(SpanUtil.toKey(t._1()), new Value(t._2()
            .toArray())));
    saveKeyValueToAccumulo(kvData);
  }

  public void saveKeyValueToAccumulo(JavaPairRDD<Key, Value> data) throws Exception {
    Job accJob = Job.getInstance(sparkCtx.hadoopConfiguration());

    if (hdfs.exists(accumuloTempDir)) {
      hdfs.delete(accumuloTempDir, true);
    }
    AccumuloFileOutputFormat.setOutputPath(accJob, accumuloTempDir);
    // must use new API here as saveAsHadoopFile throws exception
    data.saveAsNewAPIHadoopFile(accumuloTempDir.toString(), Key.class, Value.class,
        AccumuloFileOutputFormat.class, accJob.getConfiguration());
    conn.tableOperations().importDirectory(dataConfig.accumuloIndexTable,
        accumuloTempDir.toString(), failuresDir.toString(), false);
  }

  public FileSystem getHdfs() {
    return hdfs;
  }

  public Path getFluoTempDir() {
    return fluoTempDir;
  }

  public Path getAccumuloTempDir() {
    return accumuloTempDir;
  }

  public Path getHadoopTempDir() {
    return hadoopTempDir;
  }

  public Path getFailuresDir() {
    return failuresDir;
  }

  public JavaSparkContext getSparkCtx() {
    return sparkCtx;
  }

  public Connector getAccumuloConnector() {
    return conn;
  }

  public FluoConfiguration getFluoConfig() {
    return fluoConfig;
  }
}
