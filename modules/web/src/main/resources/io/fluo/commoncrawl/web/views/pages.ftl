<#include "common/header.ftl">
<#if pages.pages?has_content>
<div class="row">
  <div class="col-md-6">
    <form action="pages" method="get">
    <div class="input-group">
      <input type="text" class="form-control" name="domain" value="${pages.domain?html}">
      <span class="input-group-btn">
        <button class="btn btn-default" type="submit">Search</button>
       </span>
    </div>
    </form>
  </div>
</div>
<div class="row">
  <div class="col-md-6">
    <h4>Page ${pages.pageNum+1} of ${pages.total} results</h4>
  </div>
  <div class="col-md-6">
  <#if (pages.next?length > 0)>
    <a class="btn btn-default pull-right" href="/pages?domain=${pages.domain?url}&next=${pages.next?url}&pageNum=${pages.pageNum+1}">Next</a>
  </#if>
  <#if (pages.pageNum - 1 >= 0)>
    <a class="btn btn-default pull-right" href="/pages?domain=${pages.domain?url}&pageNum=${pages.pageNum - 1}">Previous</a>
  </#if>
  </div>
</div>
<div class="row">
  <div class="col-md-12">
    <table class="table table-striped">
    <thead><th>Score</th><th>URL</th></thead>
    <#list pages.pages as page>
      <tr>
        <td>${page.score?html}</td>
        <td><a href="/page?url=${page.url?url}">${page.url?html}</a></td>
      </tr>
    </#list>
    </table>
  </div>
</div>
<#else>
<div class="row">
  <div class="col-md-12">
    <h3>No results for ${pages.domain?html}</h3>
  </div>
</div>
</#if>
<#include "common/footer.ftl">
