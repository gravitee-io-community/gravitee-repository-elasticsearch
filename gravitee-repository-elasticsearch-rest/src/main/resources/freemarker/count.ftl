{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
<#if countQuery.query()?has_content>
        {
          "query_string": {
            "query": "${countQuery.query().filter()}"
          }
        },
</#if>
<#if countQuery.root()?has_content>
        {
          "term": {
            "${countQuery.root().field()}": "${countQuery.root().id()}"
          }
        },
</#if>
        {
          "range": {
            "@timestamp": {
              "from": ${countQuery.timeRange().range().from()},
              "to": ${countQuery.timeRange().range().to()},
              "include_lower": true,
              "include_upper": true
            }
          }
        }
      ]
    }
  }
}