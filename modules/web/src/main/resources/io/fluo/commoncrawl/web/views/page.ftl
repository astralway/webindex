<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.PageView" -->
<#setting url_escaping_charset='ISO-8859-1'>
<html>
<body>
  <a href="/site?domain=${page.domain?url}">Back to domain results</a>
  <#if page.links?has_content>
    <h3>Inbound links to page: ${page.url?html}</h3>
    <#if (page.pageNum - 1 >= 0)>
      <a href="/page?url=${page.url?url}&pageNum=${page.pageNum - 1}&domain=${page.domain?url}">Previous</a>
    </#if>
    <#if (page.next?length > 0)>
      <a href="/page?url=${page.url?url}&next=${page.next?url}&pageNum=${page.pageNum+1}&domain=${page.domain?url}">Next</a>
    </#if>
    <table border="1">
      <thead><th>Anchor Text</th><th>URL</th></thead>
      <#list page.links as link>
        <tr><td>${link.anchorText?html}</td><td>${link.url?html}</td></tr>
      </#list>
    </table>
  <#else>
    <h3>No inbound links to page: ${page.url?html}</h3>
  </#if>
</body>
</html>
