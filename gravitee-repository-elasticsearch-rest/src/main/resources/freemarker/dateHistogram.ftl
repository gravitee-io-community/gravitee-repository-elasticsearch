{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
<#if histogramQuery.query()?has_content>
        {
          "query_string": {
            "query": "${histogramQuery.query().filter()}"
          }
        },
</#if>
<#if histogramQuery.root()?has_content>
        {
          "term": {
            "${histogramQuery.root().field()}": "${histogramQuery.root().id()}"
          }
        },
</#if>
        {
          "range": {
            "@timestamp": {
              "from": ${histogramQuery.timeRange().range().from()},
              "to": ${histogramQuery.timeRange().range().to()},
              "include_lower": true,
              "include_upper": true
            }
          }
        }
      ]
    }
  },
  "aggregations": {
    "by_date": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "${histogramQuery.timeRange().interval().toMillis()}ms",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": ${histogramQuery.timeRange().range().from()},
          "max": ${histogramQuery.timeRange().range().to()}
        }
      }
<#if histogramQuery.aggregations()?has_content>
      ,
      "aggregations": {
  <#list histogramQuery.aggregations() as aggregation>
    <#switch aggregation.type()>
      <#case "AVG">
      "avg_${aggregation.field()}": {
        "avg": {
          "field": "${aggregation.field()}"
        }
      }
        <#break>
      <#case "FIELD">
      "by_${aggregation.field()}": {
        "terms": {
          "field": "${aggregation.field()}"
        }
      }
        <#break>
      <#default>
        <#break>
    </#switch>
    <#sep>,</#sep>
  </#list>
      }
</#if>
    }
  }
}