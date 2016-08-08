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
}
