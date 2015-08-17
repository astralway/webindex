package io.fluo.commoncrawl.core;

public class ColumnConstants {

  // Column Families
  // for page
  public static final String PAGE = "page";
  public static final String CRAWLS = "crawls";
  public static final String INLINKS = "inlinks";
  public static final String OUTLINKS = "outlinks";
  // for domains
  public static final String DOMAIN = "domain";
  public static final String PAGES = "pages";
  public static final String RANK = "rank";

  // Column Qualifiers
  // for page
  public static final String INLINKCOUNT = "inlinkcount";
  public static final String OUTLINKCOUNT = "outlinkcount";
  public static final String PAGESCORE = "pagescore";
  public static final String NEW = "new";
  public static final String CUR = "cur";
  // for domains
  public static final String PAGECOUNT = "pagecount";
}
