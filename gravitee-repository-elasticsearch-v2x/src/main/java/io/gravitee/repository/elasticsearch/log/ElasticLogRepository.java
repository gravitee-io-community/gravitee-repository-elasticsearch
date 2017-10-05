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
package io.gravitee.repository.elasticsearch.log;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.query.tabular.TabularQuery;
import io.gravitee.repository.analytics.query.tabular.TabularResponse;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.log.api.LogRepository;
import io.gravitee.repository.log.model.ExtendedLog;
import io.gravitee.repository.log.model.Log;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ElasticLogRepository extends AbstractElasticRepository implements LogRepository {

    private static final String [] EXCLUDED_FIELDS = new String [] { "*.client", "*.proxy" };

    @Override
    public TabularResponse query(TabularQuery query) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = prepare(query);
        requestBuilder.setFetchSource(null, EXCLUDED_FIELDS);
        return (TabularResponse) execute(requestBuilder, toTabularResponse());
    }

    @Override
    public ExtendedLog findById(String logId) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = createRequest(TYPE_REQUEST);

        BoolQueryBuilder boolQueryBuilder = boolQuery();
        boolQueryBuilder.filter(termQuery(FIELD_REQUEST_ID, logId));

        requestBuilder
                .setQuery(boolQueryBuilder)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setSize(1);
        SearchResponse searchResponse = requestBuilder.get();

        if (searchResponse.getHits().getTotalHits() == 0) {
            throw new AnalyticsException("Request [" + logId + "] does not exist");
        }

        Map<String, Object> source = searchResponse.getHits().getAt(0).getSource();
        return LogBuilder.createExtendedLog(source);
    }

    private SearchRequestBuilder prepare(TabularQuery tabularQuery) throws AnalyticsException {
        SearchRequestBuilder requestBuilder = init(tabularQuery);
        requestBuilder
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(tabularQuery.size())
                .setFrom((tabularQuery.page() - 1) * tabularQuery.size())
                .addSort(FIELD_TIMESTAMP, SortOrder.DESC);

        return requestBuilder;
    }

    private Function<SearchResponse, TabularResponse> toTabularResponse() {
        return response -> {
            SearchHits hits = response.getHits();
            TabularResponse tabularResponse = new TabularResponse(hits.totalHits());
            List<Log> logs = new ArrayList<>(hits.hits().length);
            for(int i = 0 ; i < hits.hits().length ; i++) {
                logs.add(LogBuilder.createLog(hits.getAt(i).getSource()));
            }
            tabularResponse.setLogs(logs);

            return tabularResponse;
        };
    }
}
