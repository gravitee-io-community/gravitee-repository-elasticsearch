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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.common.http.HttpStatusCode;
import io.gravitee.common.http.MediaType;
import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.model.HostAddress;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.Health;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.exceptions.TechnicalException;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Utility Elasticsearch Spring bean used to call Elasticsearch using the REST api.
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
public class ElasticsearchComponent {

    /** Logger. */
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchComponent.class);

    //ES PATH
	private static final String URL_STATE_CLUSTER = "/_cluster/health";
	private static final String URL_SEARCH = "/_search?ignore_unavailable=true";
	private static final String URL_TEMPLATE = "/_template";
	
	private static final String CONTENT_TYPE = MediaType.APPLICATION_JSON + ";charset=UTF-8";

	/**
	 * Configuration of Elasticsearch (cluster name, addresses, ...)
	 */
	@Autowired
	private ElasticConfiguration configuration;
	
	/**
	 * Tempplating tool.
	 */
	@Autowired
	private FreeMarkerComponent freeMarkerComponent;
	
	/**
	 * HTTP client.
	 */
	//TODO use vertx
    private AsyncHttpClient asyncHttpClient;
    
    /**
     * JSON mapper.
     */
    private ObjectMapper mapper;
    
    /**
     * Authorization header if Elasticsearch is protected.
     */
    private String authorizationHeader;

    /**
     * Initialize the Async REST client.
     */
    @PostConstruct
	private void init() {
	    this.asyncHttpClient = new DefaultAsyncHttpClient();
	    this.mapper = new ObjectMapper();
	    
	    // Use the ElasticConfiguration class to define username and password for ES
	    // For example if Elasticsearch is protected by nginx or x-pack
	    if (this.configuration.getUsername() != null) {
	    	this.authorizationHeader = this.initEncodedAuthorization(this.configuration.getUsername(), this.configuration.getPassword());
	    }
	}
	
	/**
	 * Create the Basic HTTP auth 
	 * @param username username
	 * @param password password
	 * @return Basic auth string
	 */
	private String initEncodedAuthorization(final String username, final String password) {
		final String auth = username + ":" + password;
		final String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
		return "Basic " + encodedAuth;
	}
		
	/**
	 * Get the Elasticsearch host
	 * @return http://<host>:<port>
	 */
	private String getHost() {
		//TODO handle loadbalancing
		final HostAddress host = this.configuration.getHostsAddresses().get(0);
		return "http://" + host.getHostname() +":" + host.getPort();
	 }
	
	/**
	 * Get the cluster health
	 * @return the cluster health
	 * @throws TechnicalException error occurs during ES call
	 */
	public Health getClusterHealth() throws TechnicalException {
		try {
			final ListenableFuture<Response> future = this.getRestGetClient(URL_STATE_CLUSTER).execute();
			final Response response = future.get();
			
			final String body = response.getResponseBody(StandardCharsets.UTF_8);
			
			if (response.getStatusCode() != HttpStatusCode.OK_200) {
				logger.error("Impossible to call Elasticsearch GET {}. Body is {}", response.getUri(), body);
				throw new TechnicalException("Impossible to call Elasticsearch. Elasticsearch response code is " + response.getStatusCode());
			}
			
			logger.debug("Response of ES for GET {} : {}", response.getUri(), body);
			return this.mapper.readValue(body, Health.class);
				
		} catch (final InterruptedException | ExecutionException | IOException e) {
			logger.error("Impossible to call Elasticsearch GET {}.", URL_STATE_CLUSTER, e);
			throw new TechnicalException("Impossible to call Elasticsearch. Error is " + e.getClass().getSimpleName(), e);

		}
	}

	/**
	 * Peform an HTTP search query
	 * @param indexes indexes names
	 * @param query json body query
	 * @return elasticsearch response
	 * @throws TechnicalException when a problem occur during the http call
	 */
	public ESSearchResponse search(final String indexes, final String query) throws TechnicalException {
		return this.search(indexes, null, query);
	}
	
	/**
	 * Peform an HTTP search query
	 * @param indexes indexes names. If null search on all indexes
	 * @param types elasticsearch document type separated by comma. If null search on all types
	 * @param query json body query
	 * @return elasticsearch response
	 * @throws TechnicalException when a problem occur during the http call
	 */
	public ESSearchResponse search(final String indexes, final String types, final String query) throws TechnicalException {
		try {
			// index can be null _search on all index
			final StringBuilder url = new StringBuilder("/")
					.append(indexes);
			if (types != null) {
				url.append("/")
					.append(types);
			}
			url.append(URL_SEARCH);
			
			final ListenableFuture<Response> future = this.getRestPostClient(url.toString()).setBody(query).execute();
			final Response response = future.get();

			final String body = response.getResponseBody(StandardCharsets.UTF_8);

			if (response.getStatusCode() != HttpStatusCode.OK_200) {
				logger.error("Impossible to call Elasticsearch POST {}. Body is {}", response.getUri(), body);
				throw new TechnicalException("Impossible to call Elasticsearch POST " + response.getUri() + ". Response is " + response.getStatusCode() );
			}

			logger.debug("Response of ES for POST {} : {}", response.getUri(), body);
			return this.mapper.readValue(body, ESSearchResponse.class);

		} catch (final Exception e) {
			logger.error("Impossible to call Elasticsearch", e);
			throw new TechnicalException("Impossible to call Elasticsearch.", e);
		}
	}

	/**
	 * Put the ES template.
	 * @throws TechnicalException when a problem occur during the http call
	 */
	public void putTemplate() throws TechnicalException {
		try {
			final Map<String, Object> data = new HashMap<>();
			data.put("indexName", this.configuration.getIndexName());
			final String body = this.freeMarkerComponent.generateFromTemplate("templateGravitee.ftl", data);
			
			logger.debug("PUT template : {}", body);
			
			
			final ListenableFuture<Response> future = this.getRestPutClient(URL_TEMPLATE + "/gravitee")
					.setBody(body)
					.execute();
			final Response response = future.get();

			final String responseText = response.getResponseBody(StandardCharsets.UTF_8);

			if (response.getStatusCode() != HttpStatusCode.OK_200) {
				logger.error("Impossible to call Elasticsearch PUT {}. Body is {}", response.getUri(), responseText);
				throw new TechnicalException("Impossible to call Elasticsearch PUT " + response.getUri() + ". Response is " + response.getStatusCode() );
			}

			logger.debug("Response of ES for PUT {} : {}", response.getUri(), responseText);
		} catch (final Exception e) {
			logger.error("Impossible to call Elasticsearch", e);
			throw new TechnicalException("Impossible to call Elasticsearch.", e);
		}
	}
	
	/**
	 * Get the POST HTTP client
	 * @param path request path
	 * @return POST HTTP client with common http headers filed
	 */
	public BoundRequestBuilder getRestPostClient(final String path) {
		final String url = this.getHost() + path;
		
		logger.debug("Try to call POST {}", url);

		final BoundRequestBuilder clientBuilder =
				this.asyncHttpClient
						.prepareRequest(new RequestBuilder().setMethod(HttpMethod.POST.name()).setUrl(url))
			.addHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE);
		return this.addCommonHeaders(clientBuilder);
	}
	
	/**
	 * Get the GET HTTP client
	 * @param path request path
	 * @return GET HTTP client with common http headers filed
	 */
	public BoundRequestBuilder getRestGetClient(final String path) {
		final String url = this.getHost() + path;
		
		logger.debug("Try to call GET {}", url);

		final BoundRequestBuilder clientBuilder =
				this.asyncHttpClient
						.prepareRequest(new RequestBuilder().setMethod(HttpMethod.GET.name()).setUrl(url));
		return this.addCommonHeaders(clientBuilder);
	}
	
	/**
	 * Get the PUT HTTP client
	 * @param path request path
	 * @return PUT HTTP client with common http headers filed
	 */
	public BoundRequestBuilder getRestPutClient(final String path) {
		final String url = this.getHost() + path;
		
		logger.debug("Try to call PUT {}", url);

		final BoundRequestBuilder clientBuilder =
			this.asyncHttpClient
					.prepareRequest(new RequestBuilder().setMethod(HttpMethod.PUT.name()).setUrl(url))
					.addHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE);
		return this.addCommonHeaders(clientBuilder);
	}
	
	/**
	 * Get the DELETE HTTP client
	 * @param path request path
	 * @return DELETE HTTP client with common http headers filed
	 */
	public BoundRequestBuilder getRestDeleteClient(final String path) {
		final String url = this.getHost() + path;
		
		logger.debug("Try to call DELETE {}", url);

		final BoundRequestBuilder clientBuilder =
				this.asyncHttpClient
						.prepareRequest(new RequestBuilder().setMethod(HttpMethod.DELETE.name()).setUrl(url));
		return this.addCommonHeaders(clientBuilder);
	}
	
	/**
	 * Add the common header to call Elasticsearch.
	 * @param builder the async builder
	 * @return async builer
	 */
	private BoundRequestBuilder addCommonHeaders(final BoundRequestBuilder builder) {
		builder.setCharset(StandardCharsets.UTF_8)
			.addHeader(HttpHeaders.ACCEPT, CONTENT_TYPE)
			.addHeader(HttpHeaders.ACCEPT_CHARSET, StandardCharsets.UTF_8.name());
		
		// basic auth
		if (this.authorizationHeader != null) {
			builder.addHeader(HttpHeaders.AUTHORIZATION, this.authorizationHeader);
		}

		return builder;
	}
}