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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.webindex.core.models.Page;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class IndexUtilTest {

  private static final Logger log = LoggerFactory.getLogger(IndexUtilTest.class);

  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", getClass().getSimpleName());
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void testDataSet1() throws Exception {
    JavaRDD<Page> pages = sc.parallelize(getPagesSet1());
    IndexStats stats = new IndexStats(sc);
    JavaPairRDD<RowColumn, Bytes> accumuloIndex = IndexUtil.createAccumuloIndex(stats, pages);
    verifyRDD(new File("src/test/resources/data/set1/accumulo-data.txt"), accumuloIndex);

    JavaPairRDD<RowColumn, Bytes> fluoIndex = IndexUtil.createFluoIndex(accumuloIndex);
    verifyRDD(new File("src/test/resources/data/set1/fluo-data.txt"), fluoIndex);

    JavaPairRDD<RowColumn, Bytes> reindex = IndexUtil.reindexFluo(fluoIndex);
    verifyRDD(new File("src/test/resources/data/set1/accumulo-data.txt"), reindex);
  }

  public String rcvToString(RowColumn rc, Bytes v) {
    Column col = rc.getColumn();
    return String.format("%s|%s|%s|%s", rc.getRow().toString(), col.getFamily().toString(), col
        .getQualifier().toString(), v.toString());
  }

  public void dump(JavaPairRDD<RowColumn, Bytes> rcb) {
    for (Tuple2<RowColumn, Bytes> tuple : rcb.collect()) {
      System.out.println(rcvToString(tuple._1(), tuple._2()));
    }
  }

  public void verifyRDD(File expected, JavaPairRDD<RowColumn, Bytes> actual) throws Exception {
    List<String> expectedList = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(expected))) {
      String line;
      while ((line = br.readLine()) != null) {
        expectedList.add(line);
      }
    }

    List<Tuple2<RowColumn, Bytes>> actualList = actual.collect();
    Assert.assertEquals(expectedList.size(), actualList.size());

    Iterator<Tuple2<RowColumn, Bytes>> actualIter = actualList.iterator();
    Iterator<String> expectedIter = expectedList.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      String exp = expectedIter.next();
      Tuple2<RowColumn, Bytes> act = actualIter.next();
      Assert.assertEquals(exp, rcvToString(act._1(), act._2()));
    }
  }

  private List<Page> getPagesSet1() {
    List<Page> pages = new ArrayList<>();
    Page pageA = new Page("http://a.com/1");
    pageA.addOutboundLink("http://b.com/1", "b1");
    pageA.addOutboundLink("http://b.com/3", "b3");
    pageA.addOutboundLink("http://c.com/1", "c1");
    Page pageB = new Page("http://b.com");
    pageB.addOutboundLink("http://c.com/1", "c1");
    pageB.addOutboundLink("http://b.com/2", "b2");
    pageB.addOutboundLink("http://b.com/3", "b3");
    pages.add(pageA);
    pages.add(pageB);
    return pages;
  }
}
