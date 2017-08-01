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
package io.gravitee.repository.elasticsearch.spring.mock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.exceptions.TechnicalException;

/**
 * Elasticsearch server for the test.
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
@Component
public class ElasticsearchNode {
	
	/**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchNode.class);

    /**
     * Use to call ES.
     */
	@Autowired
    private ElasticsearchComponent elasticsearchComponent;
	
	/**
	 * Templating tool.
	 */
	@Autowired
	private FreeMarkerComponent freeMarkerComponent;
    
	/**
	 * ES node.
	 */
	private Node node;
	
	/**
	 * Start ES.
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws TechnicalException 
	 */
	@PostConstruct
	private void init() throws InterruptedException, ExecutionException, TechnicalException {
		this.start();
		this.elasticsearchComponent.putTemplate();
		this.fill();
		Thread.sleep(2000);
	}
	
	/**
	 * Start ES node.
	 */
	private void start() {
		final Settings settings = Settings.builder().put("cluster.name", "gravitee_test")
				.put("node.name", "test")
				.put("http.port", 9200)
				.put("path.data","./target/data")
				.put("path.home","./target/data")
				.build();
		this.node = new Node(settings);
		this.node.start();
		
		logger.info("Elasticsearch server for test started");
	}
	
	/**
	 * Peform bulk request
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private void fill() throws InterruptedException, ExecutionException {
		final Map<String, Object> data = new HashMap<>();
		final Instant now = Instant.now();
		data.put("indexDateToday", Date.from(now));
		data.put("indexDateYesterday", Date.from(now.minus(1, ChronoUnit.DAYS)));
		
		final String body = this.freeMarkerComponent.generateFromTemplate("bulk.json", data);
		
		final ListenableFuture<Response> future = this.elasticsearchComponent.getRestPostClient("/_bulk")
				.setBody(body)
				.execute();
		final Response response = future.get();
		
		logger.info("Bulk data injected into ES {}", response.getResponseBody());
	}
	
	/**
	 * Stop ES
	 */
	@PreDestroy
	private void shutdown() {
		// remove all index
		this.node.client().admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();

		this.node.close();
	}
}
