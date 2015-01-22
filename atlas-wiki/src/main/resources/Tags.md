This page is a reference for the tags API provided by Atlas.

### URI

`/api/v1/tags?q=<expr>&[OPTIONS]`

## Query Parameters

### Callback (callback)

If the format is `json`, the callback is used for providing
[JSONP](http://en.wikipedia.org/wiki/JSONP) output. This parameter is
ignored for all other formats.

### Format (format)

Specifies the output format to use. The default is `png`.

<table>
  <tbody>
    <tr>
      <th>
        <p>Value</p>
      </th>
      <th>
        <p>Description</p>
      </th>
    </tr>
    <tr>
      <td>
        <p>json</p>
      </td>
      <td>
        <p>Outputs the graph data as a JSON object.</p>
      </td>
    </tr>
    <tr>
      <td>
        <p>txt</p>
      </td>
      <td>
        <p>Uses mime-type <code>text/plain</code> so it will render in the browser.</p>
      </td>
    </tr>
  </tbody>
</table>

### Limit (limit)

Maximum number of results to return before paging the response. If the response is paged a `x-nflx-atlas-next-offset` will be set to indicate the next offset. Pass the value with an [offset](#offset) param to get the next part of the list. If the header is not present there is no more data.

### Offset (offset)

If the response is paged this param is used to indicate where the next request should pick up from.

### Query (q)

Query expression used to select a set of metrics and manipulate them for
presentation in a graph. The query expression can use query
and std commands described in the reference. 


