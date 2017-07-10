<#ftl output_format="JSON">
{
	"took": ${took},
	"timed_out": false,
	"_shards": {
		"total": 15,
		"successful": 15,
		"failed": 0
	},
	"hits": {
		"total": 15,
		"max_score": 1.0,
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
		]
	},
	"aggregations": {
		"by_api": {
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count": 0,
			"buckets": [
				{
					"key": "bf19088c-f2c7-4fec-9908-8cf2c75fece4",
					"doc_count": 11
				},
				{
					"key": "48bddce0-11ea-4ff4-bddc-e011ea5ff4be",
					"doc_count": 1
				}
			]
		}
	}
}