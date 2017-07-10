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
package io.gravitee.repository.elasticsearch.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.elasticsearch.model.elasticsearch.Health;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryConfiguration;
import io.gravitee.repository.elasticsearch.spring.mock.ConfigurationTest;
import io.gravitee.repository.exceptions.TechnicalException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Test the component that calls ES
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { ConfigurationTest.class })
public class ElasticsearchComponentTest {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(ElasticsearchComponentTest.class);

	private final DateTimeFormatter dtf = java.time.format.DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneId.systemDefault());
	private final DateTimeFormatter dtf2 = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

	/**
	 * ES
	 */
	@Autowired
	private ElasticsearchComponent elasticsearchComponent;

	/**
	 * Template tool.
	 */
	@Autowired
	private FreeMarkerComponent freeMarkerComponent;

	/**
	 * Json mapper.
	 */
	private ObjectMapper mapper = new ObjectMapper();

	/**
	 * Test health
	 * 
	 * @throws TechnicalException
	 */
	@Test
	public void testGenerateFromTemplateWithoutData() throws TechnicalException {
		// do the call
		final Health health = this.elasticsearchComponent.getClusterHealth();

		// assert
		Assert.assertEquals("gravitee_test", health.getClusterName());
	}

	/**
	 * test search
	 * 
	 * @throws TechnicalException
	 * @throws IOException
	 */
	@Test
	public void testSearch() throws TechnicalException, IOException {
		// do the call
		final Map<String, Object> parameter = new HashMap<>();
		parameter.put("indexDateToday", new Date());
		final String query = this.freeMarkerComponent.generateFromTemplate("esQuery.json", parameter);
		logger.debug("query is {}", query);

		final ESSearchResponse result = this.elasticsearchComponent.search("_all", query);

		// assert
		final Map<String, Object> data = new HashMap<>();
		data.put("took", result.getTook());
		data.put("index", "gravitee-" + dtf.format(new Date().toInstant()));
		data.put("today", dtf2.format(new Date().toInstant()));
		final String expectedResponse = this.freeMarkerComponent.generateFromTemplate("esResponse.json", data);
		final ESSearchResponse expectedEsSearchResponse = this.mapper.readValue(expectedResponse,
				ESSearchResponse.class);

		Assert.assertEquals(this.mapper.writeValueAsString(expectedEsSearchResponse),
				this.mapper.writeValueAsString(result));
	}
}
