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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import webindex.core.models.Link;
import webindex.core.models.Page;
import webindex.core.models.URL;
import webindex.core.models.UriInfo;
import webindex.data.SparkTestUtil;

public class IndexUtilTest {

  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = SparkTestUtil.getSparkContext(getClass().getSimpleName());
  }

  @After
  public void tearDown() {
    sc.close();
    sc = null;
  }

  @Test
  public void testDataSet1() throws Exception {
    // Create pages
    JavaRDD<Page> pages = sc.parallelize(getPagesSet1());
    IndexStats stats = new IndexStats(sc);

    // Create an Accumulo index from pages and verify
    JavaPairRDD<String, UriInfo> uriMap = IndexUtil.createUriMap(pages);
    JavaPairRDD<String, Long> domainMap = IndexUtil.createDomainMap(uriMap);
    JavaPairRDD<RowColumn, Bytes> accumuloIndex =
        IndexUtil.createAccumuloIndex(stats, pages, uriMap, domainMap).sortByKey();
    verifyRDD("data/set1/accumulo-data.txt", accumuloIndex);

    // Use Accumulo index to create Fluo index and verify
    JavaPairRDD<RowColumn, Bytes> fluoIndex =
        IndexUtil.createFluoTable(pages, uriMap, domainMap, 119).sortByKey();
    verifyRDD("data/set1/fluo-data.txt", fluoIndex);

    // Use Fluo index to create Accumulo index and verify
    // JavaPairRDD<RowColumn, Bytes> accumuloIndexRecreated =
    // IndexUtil.createAccumuloIndex(fluoIndex);
    // verifyRDD("data/set1/accumulo-data.txt", accumuloIndexRecreated);
  }

  public void dump(JavaPairRDD<RowColumn, Bytes> rcb) {
    rcb.foreach(t -> System.out.println(Hex.encNonAscii(t, "|")));
  }

  public void verifyRDD(String expectedFilename, JavaPairRDD<RowColumn, Bytes> actual)
      throws Exception {
    List<String> expectedList = new ArrayList<>();
    InputStream is = getClass().getClassLoader().getResourceAsStream(expectedFilename);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
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
      Assert.assertEquals(exp, Hex.encNonAscii(act, "|"));
    }
  }

  private List<Page> getPagesSet1() {
    List<Page> pages = new ArrayList<>();
    Page pageA = new Page(URL.from("http://a.com/1").toUri());
    pageA.addOutbound(Link.of(URL.from("http://b.com/1"), "b1"));
    pageA.addOutbound(Link.of(URL.from("http://b.com/3"), "b3"));
    pageA.addOutbound(Link.of(URL.from("http://c.com/1"), "c1"));
    Page pageB = new Page(URL.from("http://b.com").toUri());
    pageB.addOutbound(Link.of(URL.from("http://c.com/1"), "c1"));
    pageB.addOutbound(Link.of(URL.from("http://b.com/2"), "b2"));
    pageB.addOutbound(Link.of(URL.from("http://b.com/3"), "b3"));
    pages.add(pageA);
    pages.add(pageB);
    return pages;
  }
}
