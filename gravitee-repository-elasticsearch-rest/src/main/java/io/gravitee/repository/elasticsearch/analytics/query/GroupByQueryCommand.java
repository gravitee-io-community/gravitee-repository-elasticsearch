/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.elasticsearch.analytics.query;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.query.Query;
import io.gravitee.repository.analytics.query.groupby.GroupByQuery;
import io.gravitee.repository.analytics.query.groupby.GroupByResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.exceptions.TechnicalException;

/**
 * Command used to handle GroupByQuery.
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
public class GroupByQueryCommand extends AstractElasticsearchQueryCommand<GroupByResponse> {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(GroupByQueryCommand.class);

	private final static String TEMPLATE = "groupBy.ftl";

	@Override
	public Class<? extends Query<GroupByResponse>> getSupportedQuery() {
		return GroupByQuery.class;
	}

	@Override
	public GroupByResponse executeQuery(Query<GroupByResponse> query) throws AnalyticsException {
		final GroupByQuery groupByQuery = (GroupByQuery) query;

		final String request = this.createQuery(TEMPLATE, query);

		try {
			final Long from = groupByQuery.timeRange().range().from();
			final Long to = groupByQuery.timeRange().range().to();
			
			final ESSearchResponse result = this.elasticsearchComponent.search(this.elasticsearchIndexUtil.getIndexName(from, to), request);
			return this.toGroupByResponse(result);
		} catch (TechnicalException e) {
			logger.error("Impossible to perform GroupByQuery", e);
			throw new AnalyticsException("Impossible to perform GroupByQuery", e);
		}
	}

	private GroupByResponse toGroupByResponse(final ESSearchResponse response) {
		final GroupByResponse groupByresponse = new GroupByResponse();

		if (response.getAggregations() == null) {
			return groupByresponse;
		}

		final String aggregationName = response.getAggregations().keySet().iterator().next();
		final io.gravitee.repository.elasticsearch.model.elasticsearch.Aggregation aggregation = response
				.getAggregations().get(aggregationName);
		final String fieldName = aggregationName.split("_")[1];

		groupByresponse.setField(fieldName);

		if (aggregationName.endsWith("_range")) {

			for (final JsonNode bucket : aggregation.getBuckets()) {

				final String keyAsString = bucket.get("key").asText();
				final long docCount = bucket.get("doc_count").asLong();
				GroupByResponse.Bucket value = new GroupByResponse.Bucket(keyAsString, docCount);
				groupByresponse.values().add(value);
			}

		} else if (aggregationName.startsWith("by_")) {

			for (final JsonNode bucket : aggregation.getBuckets()) {
				final JsonNode subAggragation = this.getFirstSubAggregation(bucket);

				if (subAggragation != null) {
					final JsonNode aggValue = subAggragation.get("value");
					if (aggValue.isNumber()) {
						final String keyAsString = bucket.get("key").asText();
						GroupByResponse.Bucket value = new GroupByResponse.Bucket(keyAsString, aggValue.asLong());
						groupByresponse.values().add(value);
					}

				} else {
					final String keyAsString = bucket.get("key").asText();
					final long docCount = bucket.get("doc_count").asLong();
					final GroupByResponse.Bucket value = new GroupByResponse.Bucket(keyAsString, docCount);
					groupByresponse.values().add(value);
				}
			}
		}
		return groupByresponse;

	}

	private JsonNode getFirstSubAggregation(JsonNode bucket) {

		for (final Iterator<String> it = bucket.fieldNames(); it.hasNext();) {
			final String fieldName = it.next();
			if (fieldName.startsWith("by_") || fieldName.startsWith("avg_") || fieldName.startsWith("min_")
					|| fieldName.startsWith("max_"))
				return bucket.get(fieldName);
		}
		return null;
	}
}
