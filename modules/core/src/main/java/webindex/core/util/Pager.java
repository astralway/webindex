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

package webindex.core.util;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class Pager {

  private Scanner scanner;
  private int pageSize;
  private Range pageRange;
  private Consumer<PageEntry> entryHandler;
  private AtomicBoolean pageRead = new AtomicBoolean(false);

  public class PageEntry {

    private Key key;
    private Value value;
    private boolean isNext;

    public PageEntry(Key key, Value value, boolean isNext) {
      this.key = key;
      this.value = value;
      this.isNext = isNext;
    }

    public Key getKey() {
      return key;
    }

    public Value getValue() {
      return value;
    }

    public boolean isNext() {
      return isNext;
    }
  }

  private Pager(Scanner scanner, Range pageRange, int pageSize, Consumer<PageEntry> entryHandler) {
    this.scanner = scanner;
    this.pageRange = pageRange;
    this.pageSize = pageSize;
    this.entryHandler = entryHandler;
  }

  public void read(Key startKey) {
    if (pageRead.get() == true) {
      throw new IllegalStateException("Pager.read() cannot be called twice");
    }
    scanner.setRange(new Range(startKey, pageRange.getEndKey()));
    handleStart(scanner.iterator());
  }

  public void read(int pageNum) {
    if (pageRead.get() == true) {
      throw new IllegalStateException("Pager.read() cannot be called twice");
    }
    scanner.setRange(pageRange);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (pageNum > 0) {
      long skip = 0;
      while (skip < (pageNum * pageSize)) {
        iterator.next();
        skip++;
      }
    }
    handleStart(iterator);
  }

  private void handleStart(Iterator<Map.Entry<Key, Value>> iterator) {
    long num = 0;
    while (iterator.hasNext() && (num < (pageSize + 1))) {
      Map.Entry<Key, Value> entry = iterator.next();
      entryHandler.accept(new PageEntry(entry.getKey(), entry.getValue(), num == pageSize));
      num++;
    }
  }

  public static Pager build(Scanner scanner, Range pageRange, int pageSize,
      Consumer<PageEntry> entryHandler) {
    return new Pager(scanner, pageRange, pageSize, entryHandler);
  }
}
