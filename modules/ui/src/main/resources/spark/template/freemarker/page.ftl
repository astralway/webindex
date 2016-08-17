<#include "common/header.ftl">
<div class="row>
  <div class="col-md-8 col-md-offset-4">
    <h3>Page Info</h3>
    <table class="table table-striped">
      <#if page.crawlDate??>
        <tr><td>Title<td>${page.title!''?html}</tr>
      </#if>
      <tr><td>URL<td>${page.url?html} &nbsp;- &nbsp;<a href="${page.url?html}">Go to page</a></tr>
      <tr><td>Domain<td><a href="pages?domain=${page.domain?url}">${page.domain?html}</a></tr>
      <tr><td>Inbound links<td><a href="/links?url=${page.url?url}&linkType=in">${page.numInbound}</a></tr>
      <#if page.crawlDate??>
        <tr><td>Outbound links<td><a href="/links?url=${page.url?url}&linkType=out">${page.numOutbound}</a></tr>
        <tr><td>Server<td>${page.server!''?html}</tr>
        <tr><td>Last Crawled<td>${page.crawlDate!''?html}</tr>
      </#if>
    </table>
  </div>
</div>
<#include "common/footer.ftl">
