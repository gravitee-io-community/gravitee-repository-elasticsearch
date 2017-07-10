{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
<#if groupByQuery.query()?has_content>
        {
          "query_string": {
            "query": "${groupByQuery.query().filter()}"
          }
        },
</#if>
<#if groupByQuery.root()?has_content>
        {
          "term": {
            "${groupByQuery.root().field()}": "${groupByQuery.root().id()}"
          }
        },
</#if>
        {
          "range": {
            "@timestamp": {
              "from": ${groupByQuery.timeRange().range().from()},
              "to": ${groupByQuery.timeRange().range().to()},
              "include_lower": true,
              "include_upper": true
            }
          }
        }
      ]
    }
  },
  "aggregations": {

<#if groupByQuery.groups()?has_content>
      "by_${groupByQuery.field()}_range": {
        "range":{
          "field":"${groupByQuery.field()}",
          "ranges":[
  <#list groupByQuery.groups() as range>
            {
              "from":${range.from()},
              "to":${range.to()}
            }
    <#sep>,</#sep>
  </#list>
          ]}
      }
<#else>
      "by_${groupByQuery.field()}": {
        "terms":{
          "field":"${groupByQuery.field()}",
          "size":20
  <#if groupByQuery.sort()?has_content>
          ,"order":{
            "${groupByQuery.sort().getType().name()?lower_case}_${groupByQuery.sort().getField()}":"${groupByQuery.sort().getOrder()?lower_case}"
          }
        },
        "aggregations":{
      <#switch groupByQuery.sort().getType().name()>
          <#case "AVG">
          "avg_${groupByQuery.sort().getField()}":{
            "avg":{
              "field":"${groupByQuery.sort().getField()}"
            }
          }
          <#break>
          <#default>
              <#break>
      </#switch>
        }
  </#if>
      }
</#if>
    }
  }
}