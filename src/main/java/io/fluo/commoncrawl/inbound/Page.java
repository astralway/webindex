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

package io.fluo.commoncrawl.inbound;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Page {

  private ArchiveRecord archiveRecord;
  private JSONObject json;
  private Link link;

  private Page(ArchiveRecord archiveRecord, JSONObject json, Link link) {
    this.archiveRecord = archiveRecord;
    this.json = json;
    this.link = link;
  }

  public static Page from(ArchiveRecord archiveRecord) throws IOException, ParseException {
    byte[] rawData;
    rawData = IOUtils.toByteArray(archiveRecord, archiveRecord.available());
    JSONObject json = new JSONObject(new String(rawData));
    return new Page(archiveRecord, json, Link.from(archiveRecord.getHeader().getUrl()));
  }

  public String getMimeType() {
    return archiveRecord.getHeader().getMimetype();
  }

  public Link getLink() {
    return link;
  }

  private JSONArray getLinksArray() {
    if (!getMimeType().equals("application/json")) {
      return new JSONArray();
    }
    try {
      return json.getJSONObject("Envelope").getJSONObject("Payload-Metadata")
          .getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata")
          .getJSONArray("Links");
    } catch (JSONException e) {
      return new JSONArray();
    }
  }

  public int getNumLinks() {
    return getLinksArray().length();
  }

  public Set<Link> getLinks() {
    JSONArray array = getLinksArray();
    Set<Link> links = new HashSet<>();
    for (int i=0; i < array.length(); i++) {
      JSONObject link = array.getJSONObject(i);
      if (link.has("path") && link.get("path").equals("A@/href") && link.has("url")) {
        String text = "";
        if (link.has("text")) {
          text = link.getString("text");
        } else if (link.has("title")) {
          text = link.getString("title");
        }
        try {
          links.add(Link.from(link.getString("url"), text));
        } catch (Exception e) {
        }
      }
    }
    return links;
  }

  public Set<Link> getExternalLinks() {
    String topPrivate = getLink().getTopPrivate();
    Set<Link> links = new HashSet<>();
    for (Link link : getLinks()) {
      if (!topPrivate.equalsIgnoreCase(link.getTopPrivate())) {
        links.add(link);
      }
    }
    return links;
  }
}
