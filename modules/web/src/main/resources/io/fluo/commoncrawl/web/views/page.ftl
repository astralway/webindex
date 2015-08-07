<#setting url_escaping_charset='ISO-8859-1'>
<html>
  <body>
    <h4>Page ${page.url?html}</h4>
    <table border="1">
      <tr><td>Score<td>${page.score}</tr>
      <tr><td>Inbound links<td><a href="/links?pageUrl=${page.url?url}&linkType=in">${page.numInbound}</a></tr>
      <tr><td>Outbound links<td><a href="/links?pageUrl=${page.url?url}&linkType=out">${page.numOutbound}</a></tr>
    </table>
  </body>
</html>
