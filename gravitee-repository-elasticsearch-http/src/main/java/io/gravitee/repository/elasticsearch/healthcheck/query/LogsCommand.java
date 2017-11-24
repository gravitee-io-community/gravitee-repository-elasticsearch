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
package io.gravitee.repository.elasticsearch.healthcheck.query;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.SearchHit;
import io.gravitee.repository.elasticsearch.model.elasticsearch.SearchHits;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.healthcheck.query.Query;
import io.gravitee.repository.healthcheck.query.log.Log;
import io.gravitee.repository.healthcheck.query.log.LogsQuery;
import io.gravitee.repository.healthcheck.query.log.LogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Command used to handle AverageResponseTime.
 *
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class LogsCommand extends AstractElasticsearchQueryCommand<LogsResponse> {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(LogsCommand.class);

	private final static String TEMPLATE = "healthcheck/logs.ftl";

	@Override
	public Class<? extends Query<LogsResponse>> getSupportedQuery() {
		return LogsQuery.class;
	}

	@Override
	public LogsResponse executeQuery(Query<LogsResponse> query) throws AnalyticsException {
		final LogsQuery logsQuery = (LogsQuery) query;

		final String request = this.createQuery(TEMPLATE, logsQuery);

		try {
			final long now = System.currentTimeMillis();
			final long from = ZonedDateTime
					.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
					.minus(1, ChronoUnit.MONTHS)
					.toInstant()
					.toEpochMilli();

			final ESSearchResponse result = this.elasticsearchComponent.search(this.elasticsearchIndexUtil.getIndexName(from, now), ES_TYPE_HEALTH, request);
			return this.toLogsResponse(result);
		} catch (TechnicalException e) {
			logger.error("Impossible to perform AverageResponseTimeQuery", e);
			throw new AnalyticsException("Impossible to perform AverageResponseTimeQuery", e);
		}
	}

	private LogsResponse toLogsResponse(final ESSearchResponse response) {
		SearchHits hits = response.getSearchHits();
		LogsResponse logsResponse = new LogsResponse(hits.getTotal());

		List<Log> logs = new ArrayList<>(hits.getHits().size());
		for (SearchHit hit : hits.getHits()) {
			logs.add(LogBuilder.createLog(hit));
		}
		logsResponse.setLogs(logs);

		return logsResponse;
	}
}
