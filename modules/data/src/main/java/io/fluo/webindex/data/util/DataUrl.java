/*
 * Copyright 2016 Fluo authors (see AUTHORS)
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

import com.google.common.net.HostSpecifier;
import com.google.common.net.InternetDomainName;
import io.fluo.webindex.core.models.URL;

public class DataUrl {

  public static String domainFromHost(String host) {
    return InternetDomainName.from(host).topPrivateDomain().name();
  }

  public static boolean isValidHost(String host) {
    return HostSpecifier.isValid(host) && InternetDomainName.isValid(host)
        && InternetDomainName.from(host).isUnderPublicSuffix();
  }

  public static URL from(String rawUrl) {
    return URL.from(rawUrl, DataUrl::domainFromHost, DataUrl::isValidHost);
  }

  public static boolean isValid(String rawUrl) {
    return URL.isValid(rawUrl, DataUrl::domainFromHost, DataUrl::isValidHost);
  }
}
