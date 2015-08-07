package io.fluo.commoncrawl.core;

public class AccumuloConstants {

  // Column Families
  public static final String CRAWLS = "crawls";
  public static final String DOMAINS = "domains";
  public static final String INLINKS = "inlinks";
  public static final String OUTLINKS = "outlinks";
  public static final String PAGEDESC = "pagedesc";
  public static final String PAGES = "pages";
  public static final String STATS = "stats";

  // Column Qualifiers
  public static final String INLINKCOUNT = "inlinkcount";
  public static final String OUTLINKCOUNT = "outlinkcount";
  public static final String PAGESCORE = "pagescore";
  public static final String PAGECOUNT = "pagecount";
}
