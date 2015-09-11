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

package io.fluo.webindex.ui.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public abstract class Pager {

  private Scanner scanner;
  private int pageSize;
  private Range pageRange;

  public Pager(Scanner scanner, Range pageRange, int pageSize) {
    this.scanner = scanner;
    this.pageRange = pageRange;
    this.pageSize = pageSize;
  }

  public void getPage(Key nextKey) {
    scanner.setRange(new Range(nextKey, pageRange.getEndKey()));
    foundStart(scanner.iterator());
  }

  public void getPage(int pageNum) {
    scanner.setRange(pageRange);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (pageNum > 0) {
      long skip = 0;
      while (skip < (pageNum * pageSize)) {
        Map.Entry<Key, Value> entry = iterator.next();
        skip++;
      }
    }
    foundStart(iterator);
  }

  private void foundStart(Iterator<Map.Entry<Key, Value>> iterator) {
    long num = 0;
    while (iterator.hasNext() && (num < (pageSize + 1))) {
      Map.Entry<Key, Value> entry = iterator.next();
      if (num == pageSize) {
        foundNextEntry(entry);
      } else {
        foundPageEntry(entry);
      }
      num++;
    }
  }

  public abstract void foundPageEntry(Map.Entry<Key, Value> entry);

  public abstract void foundNextEntry(Map.Entry<Key, Value> entry);

}
