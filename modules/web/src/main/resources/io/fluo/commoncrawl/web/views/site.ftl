<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.SiteView" -->
<#setting url_escaping_charset='ISO-8859-1'>
<html>
<body>
  <#if site.pages?has_content>
    <h3>Top pages for site: ${site.domain?html}</h3>
    <#if (site.pageNum - 1 >= 0)>
      <a href="/site?domain=${site.domain?url}&pageNum=${site.pageNum - 1}">Previous</a>
    </#if>
    <#if (site.next?length > 0)>
      <a href="/site?domain=${site.domain?url}&next=${site.next?url}&pageNum=${site.pageNum+1}">Next</a>
    </#if>
    <table border="1">
      <thead><th>Inbound Links</th><th>URL</th></thead>
      <#list site.pages as page>
        <tr><td>${page.count?html}</td><td><a href="/page?url=${page.url?url}&domain=${site.domain?url}">${page.url?html}</a></td></tr>
      </#list>
    </table>
  <#else>
    <h3>No results for ${site.domain?html}</h3>
  </#if>
</body>
</html>
