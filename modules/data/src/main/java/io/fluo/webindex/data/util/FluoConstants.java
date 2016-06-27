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

package io.fluo.webindex.data.util;

import io.fluo.webindex.core.Constants;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;

public class FluoConstants {

  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column PAGE_NEW_COL = new Column(Constants.PAGE, Constants.NEW);
  public static final Column PAGE_CUR_COL = new Column(Constants.PAGE, Constants.CUR);
  public static final Column PAGE_INCOUNT_COL = new Column(Constants.PAGE, Constants.INCOUNT);
  public static final Column PAGECOUNT_COL = new Column(Constants.DOMAIN, Constants.PAGECOUNT);
}
