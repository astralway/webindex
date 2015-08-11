<#include "common/header.ftl">
<div class="row>
  <div class="col-md-8 col-md-offset-4">
    <h3>Page Info</h3>
    <table class="table table-striped">
      <tr><td>URL<td>${page.url?html} &nbsp;- &nbsp;<a href="${page.url?html}">Go to page</a></tr>
      <tr><td>Domain<td><a href="pages?domain=${page.topPrivate?url}">${page.topPrivate?html}</a></tr>
      <tr><td>Page Score<td>${page.score}</tr>
      <tr><td>Inbound links<td><a href="/links?pageUrl=${page.url?url}&linkType=in">${page.numInbound}</a></tr>
      <tr><td>Outbound links<td><a href="/links?pageUrl=${page.url?url}&linkType=out">${page.numOutbound}</a></tr>
    </table>
  </div>
</div>
<#include "common/footer.ftl">
