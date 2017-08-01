<#ftl output_format="JSON">
{
  "size": 1,
  "query": {
    "bool": {
      "filter": {
        "term": {
          "id": "${requestId}"
        }
      }
    }
  }
}