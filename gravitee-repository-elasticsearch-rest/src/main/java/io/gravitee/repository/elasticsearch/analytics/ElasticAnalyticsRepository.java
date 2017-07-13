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

import com.fasterxml.jackson.databind.JsonNode;
import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.query.AggregationType;
import io.gravitee.repository.analytics.query.DateHistogramQuery;
import io.gravitee.repository.analytics.query.Query;
import io.gravitee.repository.analytics.query.count.CountQuery;
import io.gravitee.repository.analytics.query.count.CountResponse;
import io.gravitee.repository.analytics.query.groupby.GroupByQuery;
import io.gravitee.repository.analytics.query.groupby.GroupByResponse;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.analytics.query.response.histogram.Bucket;
import io.gravitee.repository.analytics.query.response.histogram.Data;
import io.gravitee.repository.analytics.query.response.histogram.DateHistogramResponse;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.exceptions.TechnicalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.function.Function;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Titouan COMPIEGNE (titouan.compiegne at graviteesource.com)
 * @author GraviteeSource Team
 * @author Guillaume WAIGNIER
 * @author Sebastien DEVAUX
 */
public class ElasticAnalyticsRepository extends AbstractElasticRepository implements AnalyticsRepository {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticAnalyticsRepository.class);

    private final static String DATE_HISTOGRAM_TEMPLATE = "healthCheckRequest.ftl";
    private final static String GROUP_BY_TEMPLATE = "groupBy.ftl";
    private final static String COUNT_TEMPLATE = "count.ftl";

    @Autowired
    private ElasticsearchComponent elasticsearchComponent;

    @Override
    public <T extends Response> T query(Query<T> query) throws AnalyticsException {
        if (query instanceof DateHistogramQuery) {
            final Map<String, Object> data = new HashMap<>();
            data.put("histogramQuery", query);

            final String request = this.freeMarkerComponent.generateFromTemplate(DATE_HISTOGRAM_TEMPLATE, data);

            final Long from = ((DateHistogramQuery) query).timeRange().range().from();
            final Long to = ((DateHistogramQuery) query).timeRange().range().to();

            logger.debug("ES request {}", request);

            final ESSearchResponse result;
            try {
                result = this.elasticsearchComponent.search(this.getIndexName(from, to), request);
                logger.debug("ES response {}", result);
                return (T) this.execute(result, toDateHistogramResponse((DateHistogramQuery) query));
            } catch (TechnicalException e) {
                logger.error("", e);
            }
        } else if (query instanceof GroupByQuery) {
            final Map<String, Object> data = new HashMap<>();
            data.put("groupByQuery", query);

            final String request = this.freeMarkerComponent.generateFromTemplate(GROUP_BY_TEMPLATE, data);

            final Long from = ((GroupByQuery) query).timeRange().range().from();
            final Long to = ((GroupByQuery) query).timeRange().range().to();

            logger.debug("ES request {}", request);

            final ESSearchResponse result;
            try {
                result = this.elasticsearchComponent.search(this.getIndexName(from, to), request);
                logger.debug("ES response {}", result);
                return (T) this.execute(result, toGroupByResponse());
            } catch (TechnicalException e) {
                logger.error("", e);
            }
        } else if (query instanceof CountQuery) {
            final Map<String, Object> data = new HashMap<>();
            data.put("countQuery", query);

            final String request = this.freeMarkerComponent.generateFromTemplate(COUNT_TEMPLATE, data);

            final Long from = ((CountQuery) query).timeRange().range().from();
            final Long to = ((CountQuery) query).timeRange().range().to();

            logger.debug("ES request {}", request);

            final ESSearchResponse result;
            try {
                result = this.elasticsearchComponent.search(this.getIndexName(from, to), request);
                logger.debug("ES response {}", result);
                return (T) this.execute(result, toCountResponse());
            } catch (TechnicalException e) {
                logger.error("", e);
            }
        }

        return null;
    }

    private Response execute(ESSearchResponse response, Function<ESSearchResponse, ? extends Response> function) throws AnalyticsException {
    //    try {
            return function.apply(response);
    //    } catch (ElasticsearchException ese) {
    //        logger.error("An error occurs while looking for analytics with Elasticsearch", ese);
    //        throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", ese);
    //    }
    }

    private Function<ESSearchResponse, DateHistogramResponse> toDateHistogramResponse(DateHistogramQuery query) {
        return response -> {
            DateHistogramResponse dateHistogramResponse = new DateHistogramResponse();

                if (response.getAggregations() == null) {
                return dateHistogramResponse;
            }

            // Prepare data
            Map<String, Bucket> fieldBuckets = new HashMap<>();

            final io.gravitee.repository.elasticsearch.model.elasticsearch.Aggregation dateHistogram = response.getAggregations().get("by_date");
            for (JsonNode dateBucket : dateHistogram.getBuckets()) {
                final long keyAsDate = dateBucket.get("key").asLong();
                dateHistogramResponse.timestamps().add(keyAsDate);

                final Iterator<String> fieldNamesInDateBucket = dateBucket.fieldNames();
                while (fieldNamesInDateBucket.hasNext()) {
                    final String fieldNameInDateBucket = fieldNamesInDateBucket.next();
                    this.handleSubAggregation(fieldBuckets, fieldNameInDateBucket, dateBucket, keyAsDate);
                }
            }

            if (!query.aggregations().isEmpty()) {
                query.aggregations().forEach(aggregation -> {
                    String key = aggregation.type().name().toLowerCase() + '_' + aggregation.field();
                    if (aggregation.type() == AggregationType.FIELD) {
                        key = "by_" + aggregation.field();
                    }

                    dateHistogramResponse.values().add(fieldBuckets.get(key));
                });
            }

            return dateHistogramResponse;
        };
    }

    private void handleSubAggregation(final Map<String, Bucket> fieldBuckets, final String fieldNameInDateBucket, final JsonNode dateBucket, final long keyAsDate) {
        if (!fieldNameInDateBucket.startsWith("by_")
                && !fieldNameInDateBucket.startsWith("avg_")
                && !fieldNameInDateBucket.startsWith("min_")
                && !fieldNameInDateBucket.startsWith("max_")) {
            return;
        }

        final String kindAggregation = fieldNameInDateBucket.split("_")[0];
        final String fieldName = fieldNameInDateBucket.split("_")[1];

        Bucket fieldBucket = fieldBuckets.get(fieldNameInDateBucket);
        if (fieldBucket == null) {
            fieldBucket = new Bucket(fieldNameInDateBucket, fieldName);
            fieldBuckets.put(fieldNameInDateBucket, fieldBucket);
        }

        final Map<String, List<Data>> bucketData = fieldBucket.data();
        List<Data> data;

        switch (kindAggregation) {
            case "by":
                for (final JsonNode termBucket :dateBucket.get(fieldNameInDateBucket).get("buckets")) {

                    final String keyAsString = termBucket.get("key").asText();
                    data = bucketData.get(keyAsString);
                    if (data == null) {
                        data = new ArrayList<>();
                        bucketData.put(keyAsString, data);
                    }
                    data.add(new Data(keyAsDate, termBucket.get("doc_count").asLong()));
                }
                break;
            case "min":
            case "max":
            case "avg":
                final JsonNode numericBucket = dateBucket.get(fieldNameInDateBucket);
                if (numericBucket.get("value") != null && numericBucket.get("value").isNumber()) {
                    final double value = numericBucket.get("value").asDouble();
                    data = bucketData.get(fieldNameInDateBucket);
                    if (data == null) {
                        data = new ArrayList<>();
                        bucketData.put(fieldNameInDateBucket, data);
                    }
                    data.add(new Data(keyAsDate, (long) value));
                }
                break;
        }
    }

    private Function<ESSearchResponse, GroupByResponse> toGroupByResponse() {
        return response -> {
            final GroupByResponse groupByresponse = new GroupByResponse();

            if (response.getAggregations() == null) {
                return groupByresponse;
            }

            final String aggregationName = response.getAggregations().keySet().iterator().next();
            final io.gravitee.repository.elasticsearch.model.elasticsearch.Aggregation aggregation = response.getAggregations().get(aggregationName);
            final String fieldName = aggregationName.split("_")[1];

            groupByresponse.setField(fieldName);

            if (aggregationName.endsWith("_range")) {

                for (final JsonNode bucket :aggregation.getBuckets()) {

                    final String keyAsString = bucket.get("key").asText();
                    final long docCount = bucket.get("doc_count").asLong();
                    GroupByResponse.Bucket value = new GroupByResponse.Bucket(keyAsString, docCount);
                    groupByresponse.values().add(value);
                }

            } else if (aggregationName.startsWith("by_")) {

                for (final JsonNode bucket :aggregation.getBuckets()) {
                    final  JsonNode subAggragation = this.getFirstSubAggregation(bucket);

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
        };
    }

    private JsonNode getFirstSubAggregation(JsonNode bucket) {

        for (final Iterator<String> it = bucket.fieldNames(); it.hasNext();) {
            final String fieldName = it.next();
            if (fieldName.startsWith("by_") || fieldName.startsWith("avg_")
                    || fieldName.startsWith("min_") || fieldName.startsWith("max_"))
            return bucket.get(fieldName);
        }
        return null;
    }

    private Function<ESSearchResponse, CountResponse> toCountResponse() {
        return response -> {
            CountResponse countResponse = new CountResponse();
            countResponse.setCount(response.getSearchHits().getTotal());

            return countResponse;
        };
    }
}
