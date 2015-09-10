<#include "common/header.ftl">
<#if top.results?has_content>
<div class="row">
  <div class="col-md-12">
    <h3>Pages with top ${top.resultType}s</h3>
  </div>
</div>
<div class="row">
  <div class="col-md-6">
    <h4>Page ${top.pageNum+1}</h4>
  </div>
  <div class="col-md-6">
    <#if top.next??>
      <a class="btn btn-default pull-right" href="/top?resultType=${top.resultType}&next=${top.next?url}&pageNum=${top.pageNum+1}">Next</a>
    </#if>
    <#if (top.pageNum - 1 >= 0)>
      <a class="btn btn-default pull-right" href="/top?resultType=${top.resultType}&pageNum=${top.pageNum - 1}">Previous</a>
    </#if>
  </div>
</div>
<div class="row">
  <div class="col-md-12">
    <table class="table table-striped">
      <thead><th>URL</th><th>${top.resultType}</th></thead>
      <#list top.results as result>
        <tr>
          <td><a href="/page?url=${result.key?url}">${result.key?html}</a></td>
          <td>${result.value?html}</td>
        </tr>
      </#list>
    </table>
  </div>
</div>
<#else>
<div class="row">
  <div class="col-md-12">
    <h3>No results found</h3>
  </div>
</div>
</#if>
<#include "common/footer.ftl">
