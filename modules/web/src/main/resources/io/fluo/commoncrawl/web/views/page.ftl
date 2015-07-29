<#-- @ftlvariable name="" type="io.fluo.commoncrawl.web.views.PageView" -->
<html>
<body>
  <#if page.links?has_content>
    <h3>Inbound links to page: ${page.url?html}</h3>
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
