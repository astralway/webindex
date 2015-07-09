package io.fluo.commoncrawl.inbound;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Page {

  private ArchiveRecord archiveRecord;
  private JSONObject json;

  public Page(ArchiveRecord archiveRecord) {
    this.archiveRecord = archiveRecord;
    byte[] rawData;
    try {
      rawData = IOUtils.toByteArray(archiveRecord, archiveRecord.available());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    json = new JSONObject(new String(rawData));
  }

  public String getMimeType() {
    return archiveRecord.getHeader().getMimetype();
  }

  public String getUrl() {
    return archiveRecord.getHeader().getUrl();
  }

  public String getUri() {
    return LinkUtil.transform(getUrl());
  }

  public String getDomain() {
    return LinkUtil.getDomainFromUrl(getUrl());
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

  public Map<String, String> getUrlLinks() {
    JSONArray array = getLinksArray();
    Map<String, String> links = new HashMap<>();
    for (int i=0; i < array.length(); i++) {
      JSONObject link = array.getJSONObject(i);
      if (link.has("path") && link.get("path").equals("A@/href") && link.has("url")) {
        String url = LinkUtil.clean(link.getString("url"));
        String text = "";
        if (link.has("text")) {
          text = link.getString("text");
        } else if (link.has("title")) {
          text = link.getString("title");
        }
        links.put(url, text);
      }
    }
    return links;
  }

  public Map<String, String> getExternalUriLinks() {
    String domain = getDomain().toLowerCase();
    Map<String, String> links = new HashMap<>();
    for (Map.Entry<String, String> entry : getUrlLinks().entrySet()) {
      String url = entry.getKey();
      String text = entry.getValue();
      try {
        String linkDomain = LinkUtil.getDomainFromUrl(url).toLowerCase();
        if (url.toLowerCase().startsWith("http") && !linkDomain.equals(domain)) {
          links.put(LinkUtil.transform(url), text);
        }
      } catch (Exception e) {
        continue;
      }
    }
    return links;
  }
}
