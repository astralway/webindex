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

package webindex.core;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;

public class Constants {

  // Column Families
  // for page
  public static final String PAGE = "page";
  public static final String INLINKS = "inlinks";
  // for domains
  public static final String DOMAIN = "domain";
  public static final String PAGES = "pages";
  public static final String RANK = "rank";

  // Column Qualifiers
  // for page
  public static final String INCOUNT = "incount";
  public static final String NEW = "new";
  public static final String CUR = "cur";
  // for domains
  public static final String PAGECOUNT = "pagecount";

  // Columns
  public static final Column PAGE_NEW_COL = new Column(PAGE, NEW);
  public static final Column PAGE_CUR_COL = new Column(PAGE, CUR);
  public static final Column PAGE_INCOUNT_COL = new Column(PAGE, INCOUNT);
  public static final Column PAGECOUNT_COL = new Column(DOMAIN, PAGECOUNT);

  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());
}
