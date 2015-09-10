package io.fluo.webindex.data.util;

import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.webindex.core.Constants;

public class FluoConstants {

  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column PAGE_NEW_COL = new Column(Constants.PAGE, Constants.NEW);
  public static final Column PAGE_CUR_COL = new Column(Constants.PAGE, Constants.CUR);
  public static final Column PAGE_INCOUNT_COL = new Column(Constants.PAGE, Constants.INCOUNT);
  public static final Column PAGE_SCORE_COL = new Column(Constants.PAGE, Constants.SCORE);

  public static final String INLINKS_UPDATE = "inlinks-update";
  public static final String INLINKS_CHANGE = "inlinks-change";

  public static final Column INLINKS_CHG_NTFY = new Column(FluoConstants.INLINKS_CHANGE, "ntfy");


}
