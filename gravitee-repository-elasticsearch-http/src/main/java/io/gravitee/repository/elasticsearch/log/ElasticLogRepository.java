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
import io.gravitee.repository.analytics.query.tabular.TabularQuery;
import io.gravitee.repository.analytics.query.tabular.TabularResponse;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.SearchHit;
import io.gravitee.repository.elasticsearch.model.elasticsearch.SearchHits;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.log.api.LogRepository;
import io.gravitee.repository.log.model.ExtendedLog;
import io.gravitee.repository.log.model.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 * @author Sebastien Devaux (Zenika)
 * @author Guillaume Waignier (Zenika)
 */
public class ElasticLogRepository extends AbstractElasticRepository implements LogRepository {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(ElasticLogRepository.class);

	/**
	 * Freemarker template name.
	 */
	private static final String LOG_TEMPLATE = "log/log.ftl";

	/**
	 * Freemarker template name for finding log by id.
	 */
	private static final String LOG_BY_ID_TEMPLATE = "log/logById.ftl";
	
	/**
	 * Elasticsearch document type used to perform query.
	 */
	private static final String ES_TYPE_LOG = "log";

	private final static String ES_TYPE_REQUEST = "request";

	@Override
	public TabularResponse query(final TabularQuery query) throws AnalyticsException {
		final String request = this.createElasticsearchJsonQuery(query);
		
		final Long from = query.timeRange().range().from();
		final Long to = query.timeRange().range().to();

		try {
			final ESSearchResponse result = this.elasticsearchComponent
					.search(this.elasticsearchIndexUtil.getIndexName(from, to), ES_TYPE_REQUEST, request);
			return this.toTabularResponse(result);
		} catch (final TechnicalException e) {
			logger.error("Impossible to perform log request", e);
			throw new AnalyticsException("Impossible to perform log request", e);
		}
	}

	/**
	 * Create JSON Elasticsearch query for the log
	 * @param query user query
	 * @return JSON Elasticsearch query
	 */
	private String createElasticsearchJsonQuery(final TabularQuery query) {
		final Map<String, Object> data = new HashMap<>();
		data.put("query", query);

		final String request = this.freeMarkerComponent.generateFromTemplate(LOG_TEMPLATE, data);
		logger.debug("ES request {}", request);
		return request;
	}

	@Override
	public ExtendedLog findById(final String requestId) throws AnalyticsException {
		final Map<String, Object> data = new HashMap<>();
		data.put("requestId", requestId);

		String request = this.freeMarkerComponent.generateFromTemplate(LOG_BY_ID_TEMPLATE, data);

		logger.debug("ES request {}", request);

		try {
			ESSearchResponse result = this.elasticsearchComponent.search(this.elasticsearchIndexUtil.getAllIndexName(), ES_TYPE_REQUEST, request);
			logger.debug("ES response {}", result);

			if (result.getSearchHits().getTotal() == 0) {
				throw new AnalyticsException("Request [" + requestId + "] does not exist");
			}

			SearchHit searchHit = result.getSearchHits().getHits().get(0);

			String searchHitIndex = searchHit.getIndex();

			request = this.freeMarkerComponent.generateFromTemplate(LOG_BY_ID_TEMPLATE, data);
			logger.debug("ES response {}", result);

			result = this.elasticsearchComponent.search(searchHitIndex, ES_TYPE_LOG, request);

			JsonNode log = null;
			if (result.getSearchHits().getTotal() != 0) {
				log = result.getSearchHits().getHits().get(0).getSource();
			}

			return LogBuilder.createExtendedLog(searchHit, log);
		} catch (TechnicalException e) {
			logger.error("Request [{}] does not exist", requestId, e);
			throw new AnalyticsException("Request [" + requestId + "] does not exist");
		}
	}

	private TabularResponse toTabularResponse(final ESSearchResponse response) {
		final SearchHits hits = response.getSearchHits();
		final TabularResponse tabularResponse = new TabularResponse(hits.getTotal());
		final List<Log> logs = new ArrayList<>(hits.getHits().size());
		for (int i = 0; i < hits.getHits().size(); i++) {
			logs.add(LogBuilder.createLog(hits.getHits().get(i)));
		}
		tabularResponse.setLogs(logs);

		return tabularResponse;

	}
}
