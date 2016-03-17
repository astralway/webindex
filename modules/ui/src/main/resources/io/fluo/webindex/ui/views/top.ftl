<#include "common/header.ftl">
<#if top.results?has_content>
<div class="row">
  <div class="col-md-12">
    <h3>Webpages with the most inbound links for <b>all processed data</b></h3>
  </div>
</div>
<div class="row">
  <div class="col-md-6">
    <h4>Page ${top.pageNum+1}</h4>
  </div>
  <div class="col-md-6">
    <#if top.next??>
      <a class="btn btn-default pull-right" href="/top?next=${top.next?url}&pageNum=${top.pageNum+1}">Next</a>
    </#if>
    <#if (top.pageNum - 1 >= 0)>
      <a class="btn btn-default pull-right" href="/top?pageNum=${top.pageNum-1}">Previous</a>
    </#if>
  </div>
</div>
<div class="row">
  <div class="col-md-12">
    <table class="table table-striped">
      <thead><th>Inbound Links</th><th>URL</th></thead>
      <#list top.results as result>
        <tr>
          <td class="col-md-2">${result.value?html}</td>
          <td class="col-md-10"><a href="/page?url=${result.key?url}">${result.key?html}</a></td>
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
