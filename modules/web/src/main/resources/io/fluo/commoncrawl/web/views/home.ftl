<#include "common/header.ftl">
<div class="row">
  <div class="col-md-6 col-md-offset-3" style="margin-top: 130px">
  <h2>CommonCrawl Link Search</h2>
  <p>Enter a domain below to view links to/from pages in that domain</p>
  <form action="pages" method="get">
    <div class="input-group">
      <input type="text" class="form-control" name="domain" placeholder="Example: apache.org">
      <span class="input-group-btn">
        <button class="btn btn-default" type="submit">Search</button>
       </span>
    </div>
  </form>
  </div>
</div>
<#include "common/footer.ftl">
