<#ftl output_format="JSON">
{
"mappings": {
"request": {
"properties": {
"@timestamp": {
"type": "date"
},
"api": {
"type": "keyword"
},
"api-key": {
"type": "keyword",
"index": false
},
"api-response-time": {
"type": "integer",
"index": false
},
"application": {
"type": "keyword"
},
"endpoint": {
"type": "keyword"
},
"gateway": {
"type": "keyword"
},
"local-address": {
"type": "keyword",
"index": false
},
"message": {
"type": "keyword",
"index": false
},
"method": {
"type": "short"
},
"path": {
"type": "keyword",
"index": false
},
"plan": {
"type": "keyword"
},
"proxy-latency": {
"type": "integer",
"index": false
},
"remote-address": {
"type": "keyword",
"index": false
},
"request-content-length": {
"type": "integer",
"index": false
},
"response-content-length": {
"type": "integer",
"index": false
},
"response-time": {
"type": "integer",
"index": false
},
"status": {
"type": "short"
},
"tenant": {
"type": "keyword"
},
"transaction": {
"type": "keyword"
},
"uri": {
"type": "keyword",
"index": false
}
}
},
"monitor": {
"properties": {
"gateway": {
"type": "keyword"
},
"hostname": {
"type": "keyword"
}
}
},
"log": {
"properties": {
"client-request": {
"type": "object",
"enabled": false
},
"client-response": {
"type": "object",
"enabled": false
},
"proxy-request": {
"type": "object",
"enabled": false
},
"proxy-response": {
"type": "object",
"enabled": false
}
}
},
"health": {
"properties": {
"api": {
"type": "keyword"
},
"available": {
"type": "boolean",
"index": false
},
"endpoint": {
"type": "keyword"
},
"gateway": {
"type": "keyword"
},
"response-time": {
"type": "integer",
"index": false
},
"state": {
"type": "integer",
"index": false
},
"steps": {
"type": "object",
"enabled": false
},
"success": {
"type": "boolean",
"index": false
}
}
}
},
"settings": {
"index.number_of_replicas": 1,
"index.number_of_shards": 5,
"refresh_interval": "1s"
},
"template": "${indexName}-*"
}