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

import com.fasterxml.jackson.databind.JsonNode;
import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.analytics.query.tabular.TabularQuery;
import io.gravitee.repository.analytics.query.tabular.TabularResponse;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.SearchHits;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.log.api.LogRepository;
import io.gravitee.repository.log.model.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ElasticLogRepository extends AbstractElasticRepository implements LogRepository {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticLogRepository.class);

    private final static String LOG_TEMPLATE = "log.ftl";

    private final static String LOG_BY_ID_TEMPLATE = "logById.ftl";

    @Autowired
    private ElasticsearchComponent elasticsearchComponent;

    @Override
    public TabularResponse query(TabularQuery query) throws AnalyticsException {

        final Map<String, Object> data = new HashMap<>();
        data.put("query", query);

        final String request = this.freeMarkerComponent.generateFromTemplate(LOG_TEMPLATE, data);

        final Long from = ((TabularQuery) query).timeRange().range().from();
        final Long to = ((TabularQuery) query).timeRange().range().to();

        logger.debug("ES request {}", request);

        final ESSearchResponse result;
        try {
            result = this.elasticsearchComponent.search(this.getIndexName(from, to), request);
            logger.debug("ES response {}", result);
            return (TabularResponse) this.execute(result, toTabularResponse());
        } catch (TechnicalException e) {
            logger.error("", e);
            throw new AnalyticsException(e);
        }

    }

    @Override
    public Request findById(String requestId) throws AnalyticsException {

        final Map<String, Object> data = new HashMap<>();
        data.put("requestId", requestId);

        final String request = this.freeMarkerComponent.generateFromTemplate(LOG_BY_ID_TEMPLATE, data);

        logger.debug("ES request {}", request);

        final ESSearchResponse result;
        try {
            //TODO check index name /request
            result = this.elasticsearchComponent.search(this.getAllIndexName(), request);
            logger.debug("ES response {}", result);

            if (result.getSearchHits().getTotal() == 0) {
                throw new AnalyticsException("Request [" + requestId + "] does not exist");
            }

            final JsonNode source = result.getSearchHits().getHits().get(0).getSource();

            return LogRequestBuilder.build(source, true);
        } catch (TechnicalException e) {
            logger.error("", e);
            throw new AnalyticsException("Request [" + requestId + "] does not exist");
        }
    }

    private Response execute(ESSearchResponse response, Function<ESSearchResponse, ? extends Response> function) throws AnalyticsException {
        return function.apply(response);
    }

    private Function<ESSearchResponse, TabularResponse> toTabularResponse() {
        return response -> {
            SearchHits hits = response.getSearchHits();
            TabularResponse tabularResponse = new TabularResponse(hits.getTotal());
            List<Request> requests = new ArrayList<>(hits.getHits().size());
            for(int i = 0 ; i < hits.getHits().size() ; i++) {
                requests.add(LogRequestBuilder.build(hits.getHits().get(i).getSource(), false));
            }
            tabularResponse.setRequests(requests);

            return tabularResponse;
        };
    }


}
