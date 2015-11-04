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

import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.model.Query;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ElasticAnalyticsRepository implements AnalyticsRepository {

    private final static String FIELD_API_NAME = "api-name";
    private final static String FIELD_API_KEY = "api-key";
    private final static String FIELD_RESPONSE_STATUS = "status";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_TIMESTAMP = "@timestamp";

    @Autowired
    private Client client;

    @Override
    public Object query(Query query) throws Exception {
        SearchRequestBuilder requestBuilder = client.prepareSearch("gravitee-*").setTypes("request");
        requestBuilder.setSearchType(SearchType.COUNT);

        QueryBuilder queryBuilder = null;

        // First, set the query builder
        if (query.filter() != null) {
            switch(query.filter().type()) {
                case API_KEY:
                    queryBuilder = QueryBuilders.boolQuery().must(termQuery(FIELD_API_KEY, query.filter().value()));
                    break;
                case API_NAME:
                    queryBuilder = QueryBuilders.boolQuery().must(termQuery(FIELD_API_NAME, query.filter().value()));
                    break;
            }
        }

        if (queryBuilder == null) {
            queryBuilder = matchAllQuery();
        }

        // Second, set the range query
        RangeFilterBuilder rangeFilterBuilder = rangeFilter(FIELD_TIMESTAMP).from(query.dateRange().start()).to(query.dateRange().end());

        // Finally set the query
        requestBuilder.setQuery(filteredQuery(queryBuilder, rangeFilterBuilder));

        // Calculate aggregation
        AggregationBuilder aggregationBuilder = terms("by_api").field(FIELD_API_NAME).size(0);
        AggregationBuilder subAggregationbuilder = aggregationBuilder.subAggregation(
                dateHistogram("by_date")
                        .extendedBounds(query.dateRange().start(), query.dateRange().end())
                        .field(FIELD_TIMESTAMP)
                        .interval(query.interval().toMillis())
                        .minDocCount(0));

        switch (query.type()) {
            case HITS_BY_LATENCY:
                subAggregationbuilder.subAggregation(terms("by_latency").field(FIELD_RESPONSE_TIME).size(0));
                break;
            case HITS_BY_STATUS:
                subAggregationbuilder.subAggregation(terms("by_status").field(FIELD_RESPONSE_STATUS).size(0));
                break;
        }

        // And set aggregation to the request
        requestBuilder.addAggregation(aggregationBuilder);

        // And get the response from ES
        SearchResponse response = requestBuilder.get();

        return response;
    }
}
