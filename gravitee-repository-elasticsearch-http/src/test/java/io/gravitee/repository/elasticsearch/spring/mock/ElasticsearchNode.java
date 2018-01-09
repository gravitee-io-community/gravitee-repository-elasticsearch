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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.exceptions.TechnicalException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Elasticsearch server for the test.
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
public class ElasticsearchNode {
	
	/**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchNode.class);

	/**
	 * Templating tool.
	 */
	@Autowired
	private FreeMarkerComponent freeMarkerComponent;
    
	/**
	 * ES node.
	 */
	private Node node;

	private int httpPort;

	/**
	 * Start ES.
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws TechnicalException 
	 */
	@PostConstruct
	private void init() throws Exception {
		this.start();
		this.fill();
		Thread.sleep(2000);
	}
	
	/**
	 * Start ES node.
	 */
	private void start() throws NodeValidationException {
		this.httpPort = generateFreePort();

		final Settings settings = Settings.builder()
				.put("cluster.name", "gravitee_test")
				.put("node.name", "test")
				.put("http.type", "netty4")
				.put("http.port", httpPort)
				.put("path.data","./target/data")
				.put("path.home","./target/data")
				.build();

		this.node = new PluginConfigurableNode(
				settings,
				Collections.singletonList(Netty4Plugin.class)).start();

		this.node.start();
		
		logger.info("Elasticsearch server for test started");
	}
	
	/**
	 * Perform bulk request
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private void fill() throws InterruptedException, ExecutionException, URISyntaxException {
		ObjectMapper mapper = new ObjectMapper(); // create once, reuse

		final Map<String, Object> data = new HashMap<>();
		final Instant now = Instant.now();
		data.put("indexName", "gravitee");
		data.put("indexDateToday", Date.from(now));
		data.put("indexDateYesterday", Date.from(now.minus(1, ChronoUnit.DAYS)));
		data.put("numberOfShards", 5);
		data.put("numberOfReplicas", 1);

		PutIndexTemplateResponse putMappingResponse = this.node.client()
				.admin()
				.indices().putTemplate(
						new PutIndexTemplateRequest("gravitee")
								.source(
										this.freeMarkerComponent.generateFromTemplate("index-template-es-5x.ftl", data))
				).get();

		logger.info("Put mapping response: {}", putMappingResponse);

		final String body = this.freeMarkerComponent.generateFromTemplate("bulk.json", data);
		String lines[] = body.split("\\r?\\n");
		for (int i = 0 ; i < lines.length - 1; i += 2) {
			String index = lines[i];
			String value = lines[i+1];

			try {
				JsonNode node = mapper.readTree(index);
				JsonNode indexNode = node.get("index");

				this.node.client()
						.prepareIndex(
								indexNode.get("_index").asText(),
								indexNode.get("_type").asText(),
								indexNode.get("_id").asText()
						)
						.setSource(value, XContentType.JSON)
						.get();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		logger.info("Bulk data injected into ES");
	}
	
	/**
	 * Stop ES
	 */
	@PreDestroy
	private void shutdown() throws IOException {
		// remove all index
		this.node.client().admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();

		this.node.close();
	}

	private int generateFreePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			int port = socket.getLocalPort();
			return port;
		} catch (IOException e) {
		}
		return -1;
	}

	public int getHttpPort() {
		return httpPort;
	}

	private static class PluginConfigurableNode extends Node {
		public PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
			super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
		}
	}
}
