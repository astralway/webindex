<html>
<#include "common/head.ftl">
<body>
<div class="container" style="margin-top: 20px">
<div class="row">
  <div class="col-md-6 col-md-offset-3" style="margin-top: 200px">
  <img src="/img/webindex.png" alt="WebIndex">
  <div style="margin-top: 25px;">
    <h4>Enter a domain to view known webpages in that domain:</h4>
  </div>
  <form action="pages" method="get">
    <div class="input-group" style="margin-top: 25px">
      <input type="text" class="form-control" name="domain" placeholder="Example: apache.org">
      <span class="input-group-btn">
        <button class="btn btn-default" type="submit">Search</button>
       </span>
    </div>
  </form>
  </div>
</div>
<div class="row">
  <div class="col-md-6 col-md-offset-3" style="margin-top: 20px">
    <p><b>Or view the webpages with the most inbound links for <a href="/top">all processed data</a>.</p>
  </div>
</div>
<#include "common/footer.ftl">
