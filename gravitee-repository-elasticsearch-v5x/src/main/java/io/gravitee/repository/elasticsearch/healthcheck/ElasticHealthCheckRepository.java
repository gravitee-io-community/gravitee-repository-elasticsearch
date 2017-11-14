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
import io.gravitee.repository.healthcheck.api.HealthCheckRepository;
import io.gravitee.repository.healthcheck.query.*;
import io.gravitee.repository.healthcheck.query.availability.AvailabilityQuery;
import io.gravitee.repository.healthcheck.query.availability.AvailabilityResponse;
import io.gravitee.repository.healthcheck.query.log.ExtendedLog;
import io.gravitee.repository.healthcheck.query.log.Log;
import io.gravitee.repository.healthcheck.query.log.LogsQuery;
import io.gravitee.repository.healthcheck.query.log.LogsResponse;
import io.gravitee.repository.healthcheck.query.responsetime.AverageResponseTimeQuery;
import io.gravitee.repository.healthcheck.query.responsetime.AverageResponseTimeResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ElasticHealthCheckRepository extends AbstractElasticRepository implements HealthCheckRepository {

    private final static String TYPE_HEALTH = "health";

    private final static String FIELD_AVAILABLE = "available";
    private final static String FIELD_RESPONSE_TIME = "response-time";

    @Override
    public <T extends Response> T query(Query<T> query) throws AnalyticsException {
        if (query instanceof AvailabilityQuery) {
            SearchRequestBuilder requestBuilder = prepare((AvailabilityQuery) query);
            return (T) apply(requestBuilder, toAvailabilityResponse());
        } else if (query instanceof AverageResponseTimeQuery) {
            SearchRequestBuilder requestBuilder = prepare((AverageResponseTimeQuery) query);
            return (T) apply(requestBuilder, toAverageResponseTimeResponse());
        } else if (query instanceof LogsQuery) {
            SearchRequestBuilder requestBuilder = prepare((LogsQuery) query);
            return (T) apply(requestBuilder, toLogsResponse());
        }

        return null;
    }

    @Override
    public ExtendedLog findById(String logId) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = createRequest(TYPE_HEALTH)
                .setQuery(org.elasticsearch.index.query.QueryBuilders.idsQuery(TYPE_HEALTH).addIds(logId))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(1);

        SearchResponse searchResponse = requestBuilder.get();

        if (searchResponse.getHits().getTotalHits() == 0) {
            throw new AnalyticsException("Health-check [" + logId + "] does not exist");
        }

        return LogBuilder.createExtendedLog(searchResponse.getHits().getAt(0));
    }

    private Function<SearchResponse, ? extends Response> toLogsResponse() {
        return response -> {
            SearchHits hits = response.getHits();
            LogsResponse logsResponse = new LogsResponse(hits.totalHits());
            List<Log> logs = new ArrayList<>(hits.hits().length);
            for(int i = 0 ; i < hits.hits().length ; i++) {
                logs.add(LogBuilder.createLog(hits.getAt(i)));
            }
            logsResponse.setLogs(logs);

            return logsResponse;
        };
    }

    private Function<SearchResponse, ? extends Response> toAverageResponseTimeResponse() {
        return response -> {
            Aggregations aggregations = response.getAggregations();
            AverageResponseTimeResponse averageResponseTime = new AverageResponseTimeResponse();

            if (aggregations == null) {
                averageResponseTime.setEndpointResponseTimes(Collections.emptyList());
                return averageResponseTime;
            }

            InternalTerms endpointsAgg = response.getAggregations().get("terms");

            // Store buckets to avoid multiple unmodifiableList to be created
            List<Terms.Bucket> endpointsBucket = endpointsAgg.getBuckets();
            List<FieldBucket<Long>> endpointsResponseTimes = new ArrayList<>(endpointsBucket.size());
            for (Terms.Bucket endpointBucket : endpointsBucket) {
                String endpointKey = (String) endpointBucket.getKey();
                FieldBucket<Long> endpoint = new FieldBucket<>(endpointKey);

                InternalDateRange dateRanges = endpointBucket.getAggregations().get("ranges");
                // Store buckets to avoid multiple unmodifiableList to be created
                List<InternalDateRange.Bucket> dateRangesBuckets = dateRanges.getBuckets();
                List<Bucket<Long>> responseTimes = new ArrayList<>(dateRangesBuckets.size());
                for (InternalDateRange.Bucket dateRange : dateRangesBuckets) {
                    String range = dateRange.getKey();
                    DateTime from = (DateTime) dateRange.getFrom();
                    InternalAvg results = dateRange.getAggregations().get("results");

                    Bucket<Long> responseTime = new Bucket<>();
                    responseTime.setFrom(from.getMillis());
                    responseTime.setKey(range);
                    responseTime.setValue((long) results.getValue());

                    responseTimes.add(responseTime);
                }

                // If all response times are equals to 0, do not include this bucket
                boolean include = responseTimes.stream().filter(longBucket -> longBucket.getValue() != 0L)
                        .findAny().isPresent();

                if (include) {
                    endpoint.setValues(responseTimes);
                    endpointsResponseTimes.add(endpoint);
                }
            }

            averageResponseTime.setEndpointResponseTimes(endpointsResponseTimes);
            return averageResponseTime;
        };
    }

    private Function<SearchResponse, AvailabilityResponse> toAvailabilityResponse() {
        return response -> {
            Aggregations aggregations = response.getAggregations();
            AvailabilityResponse endpointAvailability = new AvailabilityResponse();

            if (aggregations == null) {
                endpointAvailability.setEndpointAvailabilities(Collections.emptyList());
                return endpointAvailability;
            }

            InternalTerms endpointsAgg = response.getAggregations().get("terms");

            // Store buckets to avoid multiple unmodifiableList to be created
            List<Terms.Bucket> endpointsBucket = endpointsAgg.getBuckets();
            List<FieldBucket<Double>> endpointAvailabilities = new ArrayList<>(endpointsBucket.size());
            for (Terms.Bucket endpointBucket : endpointsBucket) {
                String endpointKey = (String) endpointBucket.getKey();
                FieldBucket<Double> endpoint = new FieldBucket<>(endpointKey);

                InternalDateRange dateRanges = endpointBucket.getAggregations().get("ranges");
                // Store buckets to avoid multiple unmodifiableList to be created
                List<InternalDateRange.Bucket> dateRangesBuckets = dateRanges.getBuckets();
                List<Bucket<Double>> availabilities = new ArrayList<>(dateRangesBuckets.size());
                for (InternalDateRange.Bucket dateRange : dateRangesBuckets) {
                    String range = dateRange.getKey();
                    DateTime from = (DateTime) dateRange.getFrom();

                    InternalTerms results = dateRange.getAggregations().get("results");

                    long successCount = 0;
                    long failureCount = 0;

                    List<Terms.Bucket> resultsBucket = results.getBuckets();

                    for (Terms.Bucket resultBucket : resultsBucket) {
                        if (resultBucket.getKeyAsString().equals(Boolean.TRUE.toString())) {
                            successCount = resultBucket.getDocCount();
                        } else {
                            failureCount = resultBucket.getDocCount();
                        }
                    }

                    double total = successCount + failureCount;
                    double percent = (total == 0) ? 100 : (successCount / total) * 100;

                    Bucket<Double> availability = new Bucket<>();
                    availability.setFrom(from.getMillis());
                    availability.setKey(range);
                    availability.setValue(percent);

                    availabilities.add(availability);
                }

                endpoint.setValues(availabilities);
                endpointAvailabilities.add(endpoint);
            }

            endpointAvailability.setEndpointAvailabilities(endpointAvailabilities);
            return endpointAvailability;
        };
    }

    private SearchRequestBuilder prepare(AvailabilityQuery availabilityQuery) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = init(availabilityQuery);

        // Endpoint aggregation
        TermsAggregationBuilder endpointAgg = terms("terms").field(
                availabilityQuery.field().name().toLowerCase()
        );

        // Date range aggregation
        AggregationBuilder dateRangeAgg = dateRange("ranges")
                .addUnboundedFrom("1m", "now-1m")
                .addUnboundedFrom("1h", "now-1h")
                .addUnboundedFrom("1d", "now-1d")
                .addUnboundedFrom("1w", "now-1w")
                .addUnboundedFrom("1M", "now-1M")
                .field(FIELD_TIMESTAMP);

        endpointAgg.subAggregation(dateRangeAgg);
        dateRangeAgg.subAggregation(terms("results").field(FIELD_AVAILABLE));

        // And set aggregation to the request
        requestBuilder.addAggregation(endpointAgg);

        return requestBuilder;
    }

    private SearchRequestBuilder prepare(AverageResponseTimeQuery averageResponseTimeQuery) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = init(averageResponseTimeQuery);

        // Endpoint aggregation
        TermsAggregationBuilder endpointAgg = terms("terms").field(
                averageResponseTimeQuery.field().name().toLowerCase()
        );

        // Date range aggregation
        AggregationBuilder dateRangeAgg = dateRange("ranges")
                .addUnboundedFrom("1m", "now-1m")
                .addUnboundedFrom("1h", "now-1h")
                .addUnboundedFrom("1d", "now-1d")
                .addUnboundedFrom("1w", "now-1w")
                .addUnboundedFrom("1M", "now-1M")
                .field(FIELD_TIMESTAMP);

        endpointAgg.subAggregation(dateRangeAgg);
        dateRangeAgg.subAggregation(avg("results").field(FIELD_RESPONSE_TIME));

        // And set aggregation to the request
        requestBuilder.addAggregation(endpointAgg);

        return requestBuilder;
    }

    private SearchRequestBuilder prepare(LogsQuery query) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = init(query);

        requestBuilder
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(query.size())
                .setFrom((query.page() - 1) * query.size())
                .addSort(FIELD_TIMESTAMP, SortOrder.DESC);

        return requestBuilder;
    }

    private SearchRequestBuilder init(AbstractQuery query) {
        long now = System.currentTimeMillis();
        long from = ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                .minus(1, ChronoUnit.MONTHS)
                .toInstant()
                .toEpochMilli();

        SearchRequestBuilder requestBuilder = createRequest(TYPE_HEALTH, from, now);

        BoolQueryBuilder boolQueryBuilder = boolQuery();
        if (query.root() != null) {
            boolQueryBuilder.filter(termQuery(query.root().field(), query.root().id()));
        }

        if (query.query() != null) {
            boolQueryBuilder.filter(queryStringQuery(query.query().filter()));
        }

        // Set the query
        requestBuilder.setQuery(boolQueryBuilder);

        return requestBuilder;
    }

    private Response apply(SearchRequestBuilder request, Function<SearchResponse, ? extends Response> function)  throws AnalyticsException {
        try {
            logger.debug("ES request: {}", request);

            // Get the response from ES
            SearchResponse response = request.get();
            logger.debug("ES response: {}", response);

            // Convert response
            return function.apply(response);
        } catch (ElasticsearchException ese) {
            logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
            throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
        }
    }
}
