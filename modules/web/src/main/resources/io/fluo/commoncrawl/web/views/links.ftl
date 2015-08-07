<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.PageView" -->
<#setting url_escaping_charset='ISO-8859-1'>
<html>
<body>
  <#if links.links?has_content>
    <h3>Inbound links to page: ${links.url?html}</h3>
    <#if (links.pageNum - 1 >= 0)>
      <a href="/links?pageUrl=${links.url?url}&linkType=${links.linkType}&pageNum=${links.pageNum - 1}">Previous</a>
    </#if>
    <#if (links.next?length > 0)>
      <a href="/links?pageUrl=${links.url?url}&linkType=${links.linkType}&next=${links.next?url}&pageNum=${links.pageNum+1}">Next</a>
    </#if>
    <table border="1">
      <thead><th>Anchor Text</th><th>URL</th></thead>
      <#list links.links as link>
        <tr><td>${link.anchorText?html}</td><td>${link.url?html}</td></tr>
      </#list>
    </table>
  <#else>
    <h3>No inbound links to page: ${links.url?html}</h3>
  </#if>
</body>
</html>
