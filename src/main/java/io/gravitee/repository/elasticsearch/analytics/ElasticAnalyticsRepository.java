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

import io.gravitee.common.data.domain.Order;
import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.query.HitsByApiKeyQuery;
import io.gravitee.repository.analytics.query.Query;
import io.gravitee.repository.analytics.query.response.HealthResponse;
import io.gravitee.repository.analytics.query.response.HitsResponse;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.analytics.query.response.TopHitsResponse;
import io.gravitee.repository.analytics.query.response.histogram.Bucket;
import io.gravitee.repository.analytics.query.response.histogram.Data;
import io.gravitee.repository.analytics.query.response.histogram.HistogramResponse;
import io.gravitee.repository.elasticsearch.analytics.utils.DateUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Titouan COMPIEGNE (titouan.compiegne at graviteesource.com)
 * @author GraviteeSource Team
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

    private final static String FIELD_TIMESTAMP = "@timestamp";

    private final static String FIELD_HEALTH_RESPONSE_SUCCESS = "success";

    @Autowired
    private Client client;

    @Override
    public <T extends Response> T query(Query<T> query) throws AnalyticsException {
        if (query instanceof HitsByApiKeyQuery) {
            return (T) hitsByApiKey((HitsByApiKeyQuery) query);
        }

        return null;
    }

    @Override
    public HealthResponse query(String api, long interval, long from, long to) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequestBuilder("health", from, to);

            QueryBuilder queryBuilder = boolQuery().must(termQuery("api", api));

            final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // Finally set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Calculate aggregation
            AggregationBuilder byDateAggregation = dateHistogram("by_date")
                    .extendedBounds(from, to)
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

    @Override
    public HitsResponse query(String query, String key, long from, long to) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequestBuilder("request", from, to);

            QueryBuilder queryBuilder = queryStringQuery(query);
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Get the response from ES
            SearchResponse response = requestBuilder.get();

            return toHitsResponse(response, key);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }

    @Override
    public TopHitsResponse query(String query, String key, String field, Order order, long from, long to, int size) throws AnalyticsException {
        try {
            if (size <= 0) {
                size = 10;
            }

            SearchRequestBuilder requestBuilder = createRequestBuilder("request", from, to);

            QueryBuilder queryBuilder = queryStringQuery(query);
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // set the aggregation
            TermsBuilder topHitsAggregation = terms(key).field(field).size(size);

            // set the order
            setTopHitsAggregationOrder(topHitsAggregation, order);

            // set aggregation
            requestBuilder.addAggregation(topHitsAggregation);

            // Get the response from ES
            SearchResponse response = requestBuilder.get();

            return toTopHitsResponse(response, key);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }

    @Override
    public HistogramResponse query(String query, String key, String field, List<String> aggTypes, long from, long to, long interval) throws AnalyticsException {
        try {
            SearchRequestBuilder requestBuilder = createRequestBuilder("request", from, to);

            QueryBuilder queryBuilder = queryStringQuery(query);
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(FIELD_TIMESTAMP).from(from).to(to);

            // Finally set the query
            requestBuilder.setQuery(boolQuery().filter(queryBuilder).filter(rangeQueryBuilder));

            // Calculate aggregation
            AggregationBuilder byDateAggregation = dateHistogram("by_date")
                    .extendedBounds(from, to)
                    .field(FIELD_TIMESTAMP)
                    .interval(interval)
                    .minDocCount(0);

            // create hits by aggregation
            if (aggTypes != null && !aggTypes.isEmpty()) {
                aggTypes.forEach(aggType -> {
                    AbstractAggregationBuilder hitsByAggregation = buildAggregation(aggType, field);
                    if (hitsByAggregation != null) {
                        byDateAggregation.subAggregation(hitsByAggregation);
                    }
                });
            }

            // set aggregation
            requestBuilder.addAggregation(byDateAggregation);

            // Get the response from ES
            SearchResponse response = requestBuilder.get();

            return toHistogramResponse(response, key);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }

    private HistogramResponse hitsByApiKey(HitsByApiKeyQuery query) {
        SearchRequestBuilder requestBuilder = createRequestBuilder("request", query.range().start(), query.range().end());

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

        return toHistogramResponse(response, query.apiKey());
    }

    private final Map<String, Boolean> checkedIndices = new HashMap<>();

    private SearchRequestBuilder createRequestBuilder(String type, long from, long to) {
        String [] rangedIndices = DateUtils.rangedIndices(from, to)
                .stream().map(date -> "gravitee-" + date).toArray(String[]::new);

        List<String> indices = new ArrayList<>();

        for (String indice : rangedIndices) {
            boolean exists = checkedIndices.computeIfAbsent(indice,
                    indice1 -> client.admin().indices().prepareExists(indice1).execute().actionGet().isExists());
            if (exists) {
                indices.add(indice);
            }
        }
        return client
                .prepareSearch(indices.toArray(new String[indices.size()]))
                .setTypes(type)
                .setSearchType(SearchType.COUNT);
    }

    private HistogramResponse toHistogramResponse(SearchResponse searchResponse, String key) {
        HistogramResponse histogramResponse = new HistogramResponse();

        if (searchResponse.getAggregations() == null) {
            return histogramResponse;
        }

        // Prepare data
        Bucket histogramBucket = new Bucket(key);

        Histogram dateHistogram = (Histogram) searchResponse.getAggregations().iterator().next();
        for (Histogram.Bucket dateBucket : dateHistogram.getBuckets()) {
            final long keyAsDate = ((DateTime) dateBucket.getKey()).getMillis();
            histogramResponse.timestamps().add(keyAsDate);

            Iterator<Aggregation> subAggregationsIte = dateBucket.getAggregations().iterator();
            if (subAggregationsIte.hasNext()) {

                while (subAggregationsIte.hasNext()) {
                    Map<String, List<Data>> bucketData = histogramBucket.data();
                    List<Data> data;
                    Aggregation subAggregation = subAggregationsIte.next();
                    if (subAggregation instanceof InternalAggregation)
                        switch (((InternalAggregation) subAggregation).type().name()) {
                            case "terms":
                                for (Terms.Bucket subTermsBucket : ((Terms) subAggregation).getBuckets()) {
                                    data = bucketData.get(subTermsBucket.getKeyAsString());
                                    if (data == null) {
                                        data = new ArrayList<>();
                                        bucketData.put(subTermsBucket.getKeyAsString(), data);
                                    }
                                    data.add(new Data(keyAsDate, subTermsBucket.getDocCount()));
                                }
                                break;
                            case "min":
                                InternalMin internalMin = ((InternalMin) subAggregation);
                                if (Double.isFinite(internalMin.getValue())) {
                                    data = bucketData.get(internalMin.getName());
                                    if (data == null) {
                                        data = new ArrayList<>();
                                        bucketData.put(internalMin.getName(), data);
                                    }
                                    data.add(new Data(keyAsDate, (long) internalMin.getValue()));
                                }
                                break;
                            case "max":
                                InternalMax internalMax = ((InternalMax) subAggregation);
                                if (Double.isFinite(internalMax.getValue())) {
                                    data = bucketData.get(internalMax.getName());
                                    if (data == null) {
                                        data = new ArrayList<>();
                                        bucketData.put(internalMax.getName(), data);
                                    }
                                    data.add(new Data(keyAsDate, (long) internalMax.getValue()));
                                }
                                break;
                            case "avg":
                                InternalAvg internalAvg = ((InternalAvg) subAggregation);
                                if (Double.isFinite(internalAvg.getValue())) {
                                    data = bucketData.get(internalAvg.getName());
                                    if (data == null) {
                                        data = new ArrayList<>();
                                        bucketData.put(internalAvg.getName(), data);
                                    }
                                    data.add(new Data(keyAsDate, (long) internalAvg.getValue()));
                                }
                                break;
                            default:
                                // nothing to do
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

        return histogramResponse;
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

    private HitsResponse toHitsResponse(SearchResponse response, String aggregationName) {
        HitsResponse hitsResponse = new HitsResponse();
        hitsResponse.setName(aggregationName);
        hitsResponse.setHits(response.getHits().totalHits());

        return hitsResponse;
    }

    private TopHitsResponse toTopHitsResponse(SearchResponse response, String key) {
        TopHitsResponse topHitsResponse = new TopHitsResponse();
        topHitsResponse.setName(key);

        if (response.getAggregations() != null && !response.getAggregations().asList().isEmpty()) {
            Aggregation aggregation = response.getAggregations().get(key);
            if (aggregation != null) {
                Map<String, Long> values = new LinkedHashMap<>();
                Terms topHits = ((Terms) aggregation);
                topHits.getBuckets().forEach(b -> {
                    values.put(b.getKeyAsString(), b.getDocCount());
                    // find order value
                    if (b.getAggregations() != null && !b.getAggregations().asList().isEmpty() && b.getAggregations().asList().size() >= 2) {
                        Aggregation countAggregation = b.getAggregations().asList().get(0);
                        // get document count
                        long count = 0;
                        if (countAggregation instanceof InternalValueCount)  {
                            count = ((InternalValueCount) countAggregation).getValue();
                        }
                        if (count > 0) {
                            Aggregation valueAggregation = b.getAggregations().asList().get(1);
                            if (valueAggregation instanceof InternalAvg) {
                                values.put(b.getKeyAsString(), (long) ((InternalAvg) valueAggregation).getValue());
                            }
                        } else {
                            // no data
                            values.remove(b.getKeyAsString());
                        }
                    }
                });
                topHitsResponse.setValues(values);
            }
        }

        return topHitsResponse;
    }

    private void setTopHitsAggregationOrder(TermsBuilder topHitsAggregation, Order order) {
        if (order != null) {
            boolean orderDirection = (order.getDirection() == null) ? true : (Order.Direction.DESC.equals(order.getDirection()) ? false : true);
            switch (order.getMode()) {
                case AVG:
                    topHitsAggregation.order(Terms.Order.aggregation("avg_" + order.getMode().toString() + order.getProperty(), orderDirection))
                            .subAggregation(AggregationBuilders.count("count_" + order.getMode().toString() + order.getProperty()).field(order.getProperty()))
                            .subAggregation(AggregationBuilders.avg("avg_" + order.getMode().toString() + order.getProperty()).field(order.getProperty()));
                    break;
            }
        }
    }

    private AbstractAggregationBuilder buildAggregation(String aggType, String field) {
        AbstractAggregationBuilder aggregationBuilder = null;

        if (aggType != null && !aggType.trim().isEmpty()) {
            switch (aggType) {
                case "terms" :
                    aggregationBuilder = terms("by_" + field).field(field).size(0);
                    break;
                case "min" :
                    aggregationBuilder = min("min_" + field).field(field);
                    break;
                case "max" :
                    aggregationBuilder = max("max_" + field).field(field);
                    break;
                case "avg" :
                    aggregationBuilder = avg("avg_" + field).field(field);
                    break;
            }
        }

        return aggregationBuilder;
    }

}
