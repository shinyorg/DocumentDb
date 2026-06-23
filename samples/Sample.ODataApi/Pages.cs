namespace Sample.ODataApi;

/// <summary>A clickable index of example OData queries, served at the application root.</summary>
static class Pages
{
    public const string Landing = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Shiny.DocumentDb — Sample OData API</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 880px; margin: 40px auto; padding: 0 16px; color: #1e293b; }
    h1 { color: #0f172a; } h2 { margin-top: 32px; color: #4338ca; }
    code { background: #f1f5f9; padding: 1px 4px; border-radius: 4px; }
    li { margin: 6px 0; }
    a { color: #2563eb; text-decoration: none; } a:hover { text-decoration: underline; }
    .note { color: #64748b; font-size: 0.9em; }
  </style>
</head>
<body>
  <h1>Shiny.DocumentDb — Sample OData API</h1>
  <p>A SQLite-backed document store seeded with <strong>customers</strong> and <strong>orders</strong>,
     exposed as OData v4 entity sets. Click any example below.</p>

  <h2>Customers</h2>
  <ul>
    <li><a href="/odata/customers">All customers</a> — <code>/odata/customers</code></li>
    <li><a href="/odata/customers?$filter=Country eq 'CA'">Country = CA</a> — <code>$filter=Country eq 'CA'</code></li>
    <li><a href="/odata/customers?$filter=Age gt 35 and IsActive eq true">Active, age &gt; 35</a> — <code>$filter=Age gt 35 and IsActive eq true</code></li>
    <li><a href="/odata/customers?$filter=contains(Name,'Smith')">Name contains 'Smith'</a> — <code>$filter=contains(Name,'Smith')</code></li>
    <li><a href="/odata/customers?$orderby=Age desc&$top=3">Oldest 3</a> — <code>$orderby=Age desc&$top=3</code></li>
    <li><a href="/odata/customers?$select=Name,Country&$count=true">Project + count</a> — <code>$select=Name,Country&$count=true</code></li>
  </ul>

  <h2>Orders</h2>
  <ul>
    <li><a href="/odata/orders">All orders</a> — <code>/odata/orders</code></li>
    <li><a href="/odata/orders?$filter=Status eq 'Shipped'">Shipped</a> — <code>$filter=Status eq 'Shipped'</code></li>
    <li><a href="/odata/orders?$filter=Total gt 100&$orderby=Total desc">Total &gt; 100, high to low</a> — <code>$filter=Total gt 100&$orderby=Total desc</code></li>
    <li><a href="/odata/orders?$filter=CustomerName eq 'Alice Johnson'">Alice's orders</a> — <code>$filter=CustomerName eq 'Alice Johnson'</code></li>
    <li><a href="/odata/orders?$count=true&$top=2&$skip=2">Paging (skip 2, top 2, with count)</a> — <code>$count=true&$top=2&$skip=2</code></li>
  </ul>

  <p class="note">Supported: <code>$filter</code>, <code>$orderby</code>, <code>$top</code>, <code>$skip</code>,
     <code>$count</code>, <code>$select</code>. <code>$expand</code> returns 501 (documents have no relationships).</p>
</body>
</html>
""";
}
