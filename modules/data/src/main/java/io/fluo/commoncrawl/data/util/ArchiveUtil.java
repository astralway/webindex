/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.commoncrawl.data.util;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import io.fluo.commoncrawl.core.Page;
import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveUtil {

  private static final Logger log = LoggerFactory.getLogger(ArchiveUtil.class);

  public static Page buildPage(ArchiveRecord archiveRecord) throws IOException, ParseException {
    if (archiveRecord.getHeader().getMimetype().equalsIgnoreCase("application/json")) {
      byte[] rawData = IOUtils.toByteArray(archiveRecord, archiveRecord.available());
      if (rawData.length == 0) {
        return Page.EMPTY;
      }
      String jsonString = new String(rawData);
      if (jsonString.isEmpty()) {
        return Page.EMPTY;
      }
      JSONObject json;
      try {
        json = new JSONObject(new String(rawData));
      } catch (JSONException e) {
        throw new ParseException(e.getMessage(), 0);
      }
      String pageUrl = archiveRecord.getHeader().getUrl();
      if (!LinkUtil.isValid(pageUrl)) {
        return Page.EMPTY;
      }
      String pageDomain = LinkUtil.getTopPrivate(pageUrl);
      Page page = new Page(archiveRecord.getHeader().getUrl());
      if (archiveRecord.getHeader().getMimetype().equals("application/json")) {
        try {
          JSONArray array = json.getJSONObject("Envelope").getJSONObject("Payload-Metadata")
              .getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata")
              .getJSONArray("Links");
          for (int i=0; i < array.length(); i++) {
            JSONObject link = array.getJSONObject(i);
            if (link.has("path") && link.get("path").equals("A@/href") && link.has("url")) {
              String anchorText = "";
              if (link.has("text")) {
                anchorText = link.getString("text");
              } else if (link.has("title")) {
                anchorText = link.getString("title");
              }
              String linkUrl = link.getString("url");
              if (LinkUtil.isValid(linkUrl)) {
                String linkDomain = LinkUtil.getTopPrivate(linkUrl);
                if (!pageDomain.equalsIgnoreCase(linkDomain)) {
                  page.addExternalLink(linkUrl, anchorText);
                }
              }
            }
          }
        } catch (JSONException e) {
          log.debug("Exception trying retrieve links", e);
        }
      }
      return page;
    }
    return Page.EMPTY;
  }

  public static Page buildPageIgnoreErrors(ArchiveRecord record) {
    try {
      return buildPage(record);
    } catch (Exception e) {
      log.info("Exception parsing ArchiveRecord with url {} due to {}", record.getHeader().getUrl(),
               e.getMessage());
      return Page.EMPTY;
    }
  }
}
