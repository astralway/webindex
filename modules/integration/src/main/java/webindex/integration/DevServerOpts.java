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

package webindex.integration;

import com.beust.jcommander.Parameter;

public class DevServerOpts {

  @Parameter(names = {"--metrics", "-m"}, description = "Enables sending metrics to localhost:3000")
  boolean metrics = false;

  @Parameter(names = {"--pages", "-p"}, description = "Number of pages to load")
  int numPages = 1000;

  @Parameter(names = {"--templateDir", "-t"}, description = "Specifies template directory")
  String templateDir = "modules/ui/src/main/resources/spark/template/freemarker";

  @Parameter(names = {"--help", "-h"}, description = "Prints usage", help = true)
  boolean help;
}
