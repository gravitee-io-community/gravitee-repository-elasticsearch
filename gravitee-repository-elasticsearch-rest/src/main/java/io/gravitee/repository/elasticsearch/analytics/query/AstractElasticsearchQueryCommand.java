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
package io.gravitee.repository.elasticsearch.analytics.query;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import io.gravitee.repository.analytics.query.Query;
import io.gravitee.repository.analytics.query.response.Response;
import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.analytics.ElasticsearchQueryCommand;
import io.gravitee.repository.elasticsearch.utils.ElasticsearchIndexUtil;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;

/**
 * Abstract class used to execute an analytic Elasticsearch query.
 * 
 * Based on Command Design Pattern.
 * 
 * @author Guillaume Waignier (Zenika)
 * @author Sebastien Devaux (Zenika)
 *
 */
public abstract class AstractElasticsearchQueryCommand<T extends Response> implements ElasticsearchQueryCommand<T> {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(AstractElasticsearchQueryCommand.class);
	
	/**
	 * Elasticsearch component to perform HTTP request.
	 */
	@Autowired
	protected ElasticsearchComponent elasticsearchComponent;

	/**
	 * Templating component
	 */
	@Autowired
	private FreeMarkerComponent freeMarkerComponent;

	/**
	 * Util component used to compute index name.
	 */
	@Autowired
	protected ElasticsearchIndexUtil elasticsearchIndexUtil;
	
	/**
	 * Create the elasticsearch query
	 * @param templateName Freemarker template name
	 * @param query query parameter
	 * @return the elasticsearch json query
	 */
	protected String createQuery(final String templateName, final Query<T> query) {
		final Map<String, Object> data = new HashMap<>();
		data.put("query", query);
		final String request = this.freeMarkerComponent.generateFromTemplate(templateName, data);
		
		logger.debug("ES request {}", request);
		
		return request;
	}
}
