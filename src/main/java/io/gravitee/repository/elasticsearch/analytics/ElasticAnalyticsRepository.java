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
package io.gravitee.repository.elasticsearch.analytics;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.query.HitsByApiKeyQuery;
import io.gravitee.repository.analytics.query.HitsByApiQuery;
import io.gravitee.repository.analytics.query.Query;
import io.gravitee.repository.analytics.query.response.HealthResponse;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.analytics.query.response.histogram.Bucket;
import io.gravitee.repository.analytics.query.response.histogram.Data;
import io.gravitee.repository.analytics.query.response.histogram.HistogramResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ElasticAnalyticsRepository implements AnalyticsRepository {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticAnalyticsRepository.class);

    private final static String FIELD_API_NAME = "api";
    private final static String FIELD_API_KEY = "api-key";
    private final static String FIELD_RESPONSE_STATUS = "status";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_RESPONSE_CONTENT_LENGTH = "response-content-length";

    private final static String FIELD_TIMESTAMP = "@timestamp";

    @Autowired
    private Client client;

    @Override
    public <T extends Response> T query(Query<T> query) throws AnalyticsException {
        if (query instanceof HitsByApiQuery) {
            return (T) hitsByApi((HitsByApiQuery) query);
        } else if (query instanceof HitsByApiKeyQuery) {
            return (T) hitsByApiKey((HitsByApiKeyQuery) query);
        }

        return null;
    }

    @Override
    public HealthResponse query(String api, long interval, long from, long to) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequestBuilder("health");

            QueryBuilder queryBuilder = boolQuery().must(termQuery("api", api));

            final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // Finally set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Calculate aggregation
            AggregationBuilder byDateAggregation = dateHistogram("by_date")
                    .extendedBounds(from, to)
                    .field(FIELD_TIMESTAMP)
                    .interval(interval);
//                    .minDocCount(0);
            byDateAggregation.subAggregation(terms("by_status").field(FIELD_RESPONSE_STATUS).size(0));

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

    private HistogramResponse hitsByApi(HitsByApiQuery query) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequestBuilder("request");

            QueryBuilder queryBuilder;

            if (query.api() != null) {
                queryBuilder = boolQuery().must(termQuery(FIELD_API_NAME, query.api()));
            } else {
                queryBuilder = QueryBuilders.matchAllQuery();
            }

            final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(query.range().start()).to(query.range().end());

            // Finally set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Calculate aggregation
            AggregationBuilder byApiAggregation = terms("by_api").field(FIELD_API_NAME).size(0);
            AggregationBuilder byDateAggregation = dateHistogram("by_date")
                    .extendedBounds(query.range().start(), query.range().end())
                    .field(FIELD_TIMESTAMP)
                    .interval(query.interval().toMillis())
                    .minDocCount(0);
            byApiAggregation.subAggregation(byDateAggregation);

            switch (query.type()) {
                case HITS_BY_APIKEY:
                    byDateAggregation.subAggregation(terms("by_apikey").field(FIELD_API_KEY).size(0));
                    break;
                case HITS_BY_STATUS:
                    byDateAggregation.subAggregation(terms("by_status").field(FIELD_RESPONSE_STATUS).size(0));
                    break;
                case HITS_BY_LATENCY:
                    byDateAggregation.subAggregation(terms("by_latency").field(FIELD_RESPONSE_TIME).size(0));
                    break;
                case HITS_BY_PAYLOAD_SIZE:
                    byDateAggregation.subAggregation(terms("by_payload_size").field(FIELD_RESPONSE_CONTENT_LENGTH).size(0));
            }

            // And set aggregation to the request
            requestBuilder.addAggregation(byApiAggregation);

            // Get the response from ES
            SearchResponse response = requestBuilder.get();

            return toHistogramResponse(response);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }

    private HistogramResponse hitsByApiKey(HitsByApiKeyQuery query) {
        SearchRequestBuilder requestBuilder = createRequestBuilder("request");

        QueryBuilder queryBuilder = boolQuery().must(termQuery(FIELD_API_KEY, query.apiKey()));

        final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(query.range().start()).to(query.range().end());

        // Finally set the query
        requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

        // Calculate aggregation
        AggregationBuilder aggregationBuilder = terms("by_apikey").field(FIELD_API_NAME).size(0);
        AggregationBuilder subAggregationBuilder = aggregationBuilder.subAggregation(
                dateHistogram("by_date")
                        .extendedBounds(query.range().start(), query.range().end())
                        .field(FIELD_TIMESTAMP)
                        .interval(query.interval().toMillis())
                        .minDocCount(0));

        switch (query.type()) {
            case HITS_BY_STATUS:
                subAggregationBuilder.subAggregation(terms("by_status").field(FIELD_RESPONSE_STATUS).size(0));
                break;
            case HITS_BY_LATENCY:
                subAggregationBuilder.subAggregation(terms("by_latency").field(FIELD_RESPONSE_TIME).size(0));
                break;
        }

        // And set aggregation to the request
        requestBuilder.addAggregation(aggregationBuilder);

        logger.debug("ES Request: {}", requestBuilder.toString());

        // Get the response from ES
        SearchResponse response = requestBuilder.get();

        logger.debug("ES Response: {}", requestBuilder.toString());

        return toHistogramResponse(response);
    }

    private SearchRequestBuilder createRequestBuilder(String type) {
        //TODO: Select indices according to the range from query
        return client
                .prepareSearch("gravitee-*")
                .setTypes(type)
                .setSearchType(SearchType.COUNT);
    }

    private HistogramResponse toHistogramResponse(SearchResponse searchResponse) {
        HistogramResponse histogramResponse = new HistogramResponse();

        if (searchResponse.getAggregations() == null) {
            return histogramResponse;
        }

        // First aggregation is always a term aggregation (by API or APIKey)
        Terms terms = (Terms) searchResponse.getAggregations().iterator().next();

        // Prepare data
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Bucket histogramBucket = new Bucket(bucket.getKeyAsString());

            Histogram dateHistogram = bucket.getAggregations().get("by_date");
            for (Histogram.Bucket dateBucket : dateHistogram.getBuckets()) {
                final long keyAsDate = ((DateTime) dateBucket.getKey()).getMillis();
                histogramResponse.timestamps().add(keyAsDate);

                Iterator<Aggregation> subAggregationsIte = dateBucket.getAggregations().iterator();
                if (subAggregationsIte.hasNext()) {

                    while (subAggregationsIte.hasNext()) {
                        Terms subAggregation = (Terms) subAggregationsIte.next();
                        Map<String, List<Data>> bucketData = histogramBucket.data();

                        for (Terms.Bucket subTermsBucket : subAggregation.getBuckets()) {
                            List<Data> data = bucketData.get(subTermsBucket.getKeyAsString());
                            if (data == null) {
                                data = new ArrayList<>();
                                bucketData.put(subTermsBucket.getKeyAsString(), data);
                            }

                            data.add(new Data(keyAsDate, subTermsBucket.getDocCount()));
                        }
                    }
                } else {
                    Map<String, List<Data>> bucketData = histogramBucket.data();

                        List<Data> data = bucketData.get("hits");
                        if (data == null) {
                            data = new ArrayList<>();
                            bucketData.put("hits", data);
                        }

                        data.add(new Data(keyAsDate, dateBucket.getDocCount()));
                }
            }

            histogramResponse.values().add(histogramBucket);
        }

        return histogramResponse;
    }

    private HealthResponse toHealthResponse(SearchResponse searchResponse) {
        HealthResponse healthResponse = new HealthResponse();

        if (searchResponse.getAggregations() == null) {
            return healthResponse;
        }

        // First aggregation is always a date histogram aggregation
        Histogram histogram = searchResponse.getAggregations().get("by_date");

        Map<Integer, long[]> values = new HashMap<>();
        long [] timestamps = new long[histogram.getBuckets().size()];

        // Prepare data
        int idx = 0;
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            timestamps[idx] = ((DateTime) bucket.getKey()).getMillis();

            Terms terms = bucket.getAggregations().get("by_status");

            for (Terms.Bucket termBucket : terms.getBuckets()) {
                long [] valuesByStatus = values.getOrDefault(
                        Integer.parseInt(termBucket.getKeyAsString()), new long[timestamps.length]);

                valuesByStatus[idx] = termBucket.getDocCount();

                values.put(Integer.parseInt(termBucket.getKeyAsString()), valuesByStatus);
            }

            idx++;
        }

        healthResponse.timestamps(timestamps);
        healthResponse.buckets(values);

        return healthResponse;
    }
}
