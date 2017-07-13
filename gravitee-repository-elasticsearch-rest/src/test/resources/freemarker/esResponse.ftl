<#ftl output_format="JSON">
{
  "took": ${took},
  "aggregations": {
    "by_api": {
      "buckets": [
        {
          "key": "bf19088c-f2c7-4fec-9908-8cf2c75fece4",
          "doc_count": 12
        },
        {
          "key": "e2c0ecd5-893a-458d-80ec-d5893ab58d12",
          "doc_count": 4
        },
        {
          "key": "4d8d6ca8-c2c7-4ab8-8d6c-a8c2c79ab8a1",
          "doc_count": 3
        },
        {
          "key": "be0aa9c9-ca1c-4d0a-8aa9-c9ca1c5d0aab",
          "doc_count": 3
        },
        {
          "key": "48bddce0-11ea-4ff4-bddc-e011ea5ff4be",
          "doc_count": 1
        }
      ]
    }
  },
  "timed_out": false,
  "hits": {
    "total": 26,
    "hits": [
      {
        "_index": "gravitee-2017.03.25",
        "_type": "health",
        "_id": "AVsGcnBPkSadZDBaTBao",
        "_score": 1.0,
        "_source": {
          "api": "bf19088c-f2c7-4fec-9908-8cf2c75fece4",
          "status": 200,
          "url": "https://api.gravitee.io/echo/",
          "method": "GET",
          "success": true,
          "state": 0,
          "message": null,
          "hostname": "debian",
          "@timestamp": "2017-03-25T18:10:11.079+01:00"
        }
      },
      {
        "_index": "gravitee-2017.03.25",
        "_type": "health",
        "_id": "AVsGgzxGooztmMPf1gOs",
        "_score": 1.0,
        "_source": {
          "api": "bf19088c-f2c7-4fec-9908-8cf2c75fece4",
          "status": 200,
          "url": "https://api.gravitee.io/echo/",
          "method": "GET",
          "success": true,
          "state": 0,
          "message": null,
          "hostname": "debian",
          "@timestamp": "2017-03-25T18:28:30.718+01:00"
        }
      },
      {
        "_index": "gravitee-2017.03.25",
        "_type": "health",
        "_id": "AVsGgiqaTGG_RLWNOXe7",
        "_score": 1.0,
        "_source": {
          "api": "bf19088c-f2c7-4fec-9908-8cf2c75fece4",
          "status": 200,
          "url": "https://api.gravitee.io/echo/",
          "method": "GET",
          "success": true,
          "state": 0,
          "message": null,
          "hostname": "debian",
          "@timestamp": "2017-03-25T18:27:20.714+01:00"
        }
      }
    ],
    "max_score": "1.0"
  }
}