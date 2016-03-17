<#include "common/header.ftl">
<#if links.links?has_content>
<div class="row">
  <div class="col-md-12">
    <#if links.linkType == "in">
    <h4>Webpages that link to <a href="/page?url=${links.url?url}">${links.url?html}</a></h4>
    <#else>
    <h4>Outbound links from <a href="/page?url=${links.url?url}">${links.url?html}</a></h4>
    </#if>
  </div>
</div>
<div class="row">
  <div class="col-md-6">
    <h4>Page ${links.pageNum+1} of ${links.total} results</h4>
  </div>
  <div class="col-md-6">
    <#if (links.next?length > 0)>
      <a class="btn btn-default pull-right" href="/links?url=${links.url?url}&linkType=${links.linkType}&next=${links.next?url}&pageNum=${links.pageNum+1}">Next</a>
    </#if>
    <#if (links.pageNum - 1 >= 0)>
      <a class="btn btn-default pull-right" href="/links?url=${links.url?url}&linkType=${links.linkType}&pageNum=${links.pageNum - 1}">Previous</a>
    </#if>
  </div>
</div>
<div class="row">
  <div class="col-md-12">
    <table class="table table-striped">
      <thead><th>URL</th><th>Anchor Text</th></thead>
      <#list links.links as link>
        <tr>
          <td><a href="/page?url=${link.url?url}">${link.url?html}</a></td>
          <td>${link.anchorText?html}</td>
        </tr>
      </#list>
    </table>
  </div>
</div>
<#else>
<div class="row">
  <div class="col-md-12">
    <h3>No ${links.linkType?cap_first}bound links to page: ${links.url?html}</h3>
  </div>
</div>
</#if>
<#include "common/footer.ftl">
