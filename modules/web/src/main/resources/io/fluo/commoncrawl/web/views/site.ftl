<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.SiteView" -->
<#setting url_escaping_charset='ISO-8859-1'>
<html>
<body>
  <#if site.pages?has_content>
    <h3>Top pages for site: ${site.domain?html}</h3>
    <table border="1">
      <thead><th>Inbound Links</th><th>URL</th></thead>
      <#list site.pages as page>
        <tr><td>${page.count?html}</td><td><a href="/page?url=${page.url?url}">${page.url?html}</a></td></tr>
      </#list>
    </table>
  <#else>
    <h3>No results for ${site.domain?html}</h3>
  </#if>
</body>
</html>
