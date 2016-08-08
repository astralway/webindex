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

import java.io.Serializable;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(IndexUtil.class);

  private Accumulator<Integer> numPages;
  private Accumulator<Integer> numEmpty;
  private Accumulator<Integer> numExternalLinks;

  public IndexStats(JavaSparkContext ctx) {
    numPages = ctx.accumulator(0);
    numEmpty = ctx.accumulator(0);
    numExternalLinks = ctx.accumulator(0);
  }

  public void addPage(Integer num) {
    numPages.add(num);
  }

  public void addEmpty(Integer num) {
    numEmpty.add(num);
  }

  public void addExternalLinks(Integer num) {
    numExternalLinks.add(num);
  }

  public void print() {
    log.info("Num empty = {}", numEmpty.value());
    log.info("Num pages = {}", numPages.value());
    log.info("Num external links = {}", numExternalLinks.value());
  }
}
