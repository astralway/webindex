<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.SiteView" -->
<#setting url_escaping_charset='ISO-8859-1'>
<html>
<body>
  <#if pages.pages?has_content>
    <h3>Top pages for site: ${pages.domain?html}</h3>
    <p>Page ${pages.pageNum+1} of ${pages.total} results</p>
    <#if (pages.pageNum - 1 >= 0)>
      <a href="/pages?domain=${pages.domain?url}&pageNum=${pages.pageNum - 1}">Previous</a>
    </#if>
    <#if (pages.next?length > 0)>
      <a href="/pages?domain=${pages.domain?url}&next=${pages.next?url}&pageNum=${pages.pageNum+1}">Next</a>
    </#if>
    <table border="1">
      <thead><th>Score</th><th>URL</th></thead>
      <#list pages.pages as page>
        <tr>
          <td>${page.score?html}</td>
          <td><a href="/page?url=${page.url?url}">${page.url?html}</a></td>
        </tr>
      </#list>
    </table>
  <#else>
    <h3>No results for ${pages.domain?html}</h3>
  </#if>
</body>
</html>
