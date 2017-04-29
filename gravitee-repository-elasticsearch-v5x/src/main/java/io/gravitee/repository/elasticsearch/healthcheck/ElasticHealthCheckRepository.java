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
package io.gravitee.repository.elasticsearch.healthcheck;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.healthcheck.HealthCheckRepository;
import io.gravitee.repository.healthcheck.HealthResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ElasticHealthCheckRepository extends AbstractElasticRepository implements HealthCheckRepository {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticHealthCheckRepository.class);

    private final static String TYPE_HEALTH = "health";

    private final static String FIELD_HEALTH_RESPONSE_SUCCESS = "success";

    @Override
    public HealthResponse query(String api, long interval, long from, long to) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequest(TYPE_HEALTH, from, to);

            QueryBuilder queryBuilder = boolQuery().must(termQuery("api", api));

            final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // Finally set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Calculate aggregation
            AggregationBuilder byDateAggregation = dateHistogram("by_date")
                    .extendedBounds(new ExtendedBounds(from, to))
                    .field(FIELD_TIMESTAMP)
                    .interval(interval);

            byDateAggregation.subAggregation(terms("by_result").field(FIELD_HEALTH_RESPONSE_SUCCESS).size(0));

            // And set aggregation to the request
            requestBuilder.addAggregation(byDateAggregation);

            // Get the response from ES
            SearchResponse response = requestBuilder.get();

            return toHealthResponse(response);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }

    private HealthResponse toHealthResponse(SearchResponse searchResponse) {
        HealthResponse healthResponse = new HealthResponse();

        if (searchResponse.getAggregations() == null) {
            return healthResponse;
        }

        // First aggregation is always a date histogram aggregation
        Histogram histogram = searchResponse.getAggregations().get("by_date");

        Map<Boolean, long[]> values = new HashMap<>(2);
        long [] timestamps = new long[histogram.getBuckets().size()];

        // Prepare data
        int idx = 0;
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            timestamps[idx] = ((DateTime) bucket.getKey()).getMillis();

            Terms terms = bucket.getAggregations().get("by_result");

            for (Terms.Bucket termBucket : terms.getBuckets()) {
                long [] valuesByStatus = values.getOrDefault(
                        Integer.parseInt(termBucket.getKeyAsString()) == 1, new long[timestamps.length]);

                valuesByStatus[idx] = termBucket.getDocCount();

                values.put(Integer.parseInt(termBucket.getKeyAsString()) == 1, valuesByStatus);
            }

            idx++;
        }

        healthResponse.timestamps(timestamps);
        healthResponse.buckets(values);

        return healthResponse;
    }
}
