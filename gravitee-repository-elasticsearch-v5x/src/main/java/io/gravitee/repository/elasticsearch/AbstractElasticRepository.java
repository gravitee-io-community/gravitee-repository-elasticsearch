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
package io.gravitee.repository.elasticsearch;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.query.AbstractQuery;
import io.gravitee.repository.analytics.query.TimeRangeFilter;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.utils.DateUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public abstract class AbstractElasticRepository {

    /**
     * Logger.
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final static String FIELD_TIMESTAMP = "@timestamp";
    protected final static String TYPE_REQUEST = "request";
    protected final static String TYPE_LOG = "log";

    @Autowired
    protected ElasticConfiguration configuration;

    @Autowired
    protected Client client;

    protected SearchRequestBuilder createRequest(String type, long from, long to) {
        String [] rangedIndices = DateUtils.rangedIndices(from, to)
                .stream()
                .map(date -> configuration.getIndexName() + '-' + date)
                .toArray(String[]::new);

        return client
                .prepareSearch(rangedIndices)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setTypes(type)
                .setSize(0);
    }

    protected SearchRequestBuilder createRequest(String type) {
        return client
                .prepareSearch(configuration.getIndexName() + "-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setTypes(type)
                .setSize(0);
    }

    protected SearchRequestBuilder createRequest(String type, String ... indices) {
        return client
                .prepareSearch(indices)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setTypes(type)
                .setSize(0);
    }

    protected SearchRequestBuilder init(AbstractQuery query) {
        TimeRangeFilter timeRange = query.timeRange();
        SearchRequestBuilder requestBuilder =
                (timeRange != null) ?
                        createRequest(TYPE_REQUEST, timeRange.range().from(), timeRange.range().to()):
                        createRequest(TYPE_REQUEST);

        BoolQueryBuilder boolQueryBuilder = boolQuery();
        if (query.root() != null) {
            boolQueryBuilder.filter(termQuery(query.root().field(), query.root().id()));
        }

        if (query.query() != null) {
            boolQueryBuilder.filter(queryStringQuery(query.query().filter()));
        }

        // Apply date range filter
        if (timeRange != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery(FIELD_TIMESTAMP)
                    .from(timeRange.range().from())
                    .to(timeRange.range().to()));
        }

        // Set the query
        requestBuilder.setQuery(boolQueryBuilder);

        return requestBuilder;
    }

    protected Response execute(SearchRequestBuilder request, Function<SearchResponse, ? extends Response> function)  throws AnalyticsException {
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
