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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unitils.reflectionassert.ReflectionAssert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.monitoring.ElasticMonitoringRepository;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryConfiguration;
import io.gravitee.repository.elasticsearch.spring.mock.ConfigurationTest;
import io.gravitee.repository.elasticsearch.spring.mock.PropertySourceRepositoryInitializer;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.monitoring.model.MonitoringResponse;

/**
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {AnalyticsRepositoryConfiguration.class,ConfigurationTest.class},
        initializers = PropertySourceRepositoryInitializer.class)
public class ElasticMonitoringRepositoryTest {

    /** Logger. */
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchComponent.class);

    @Autowired
    private ElasticMonitoringRepository elasticMonitoringRepository;

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

    @Before
    public void setUp() throws ExecutionException, InterruptedException {

        final Map<String, Object> data = new HashMap<>();
        data.put("indexDate", new Date());

        final String body = this.freeMarkerComponent.generateFromTemplate("bulkMonitoring.json", data);

        final ListenableFuture<Response> future = this.elasticsearchComponent.getRestPostClient("/_bulk")
                .setBody(body)
                .execute();
        final Response response = future.get();

        logger.info("Bulk data injected into ES {}", response.getResponseBody());
        Thread.sleep(3000);
        
    }
    
    /**
     * Remove mocked ES data
     * @throws InterruptedException 
     * @throws ExecutionException 
     */
    @After
    public void tearDown() throws InterruptedException, ExecutionException {
    	final LocalDate today = LocalDate.now();
    	final String formatedDate = today.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
    	
    	final ListenableFuture<Response> responseDelete1 = this.elasticsearchComponent.getRestDeleteClient("/gravitee-" + formatedDate + "/monitor/AVsFOS2okSadZDBaTAbl").execute();
    	final ListenableFuture<Response> responseDelete2 = this.elasticsearchComponent.getRestDeleteClient("/gravitee-" + formatedDate + "/monitor/AVsFOXvXkSadZDBaTAbp").execute();
    	final ListenableFuture<Response> responseDelete3 = this.elasticsearchComponent.getRestDeleteClient("/gravitee-" + formatedDate + "/monitor/AVsFOUE0kSadZDBaTAbm").execute();
    	
    	logger.debug("ES response {}", responseDelete1.get().getResponseBody(StandardCharsets.UTF_8));
    	logger.debug("ES response {}", responseDelete2.get().getResponseBody(StandardCharsets.UTF_8));
    	logger.debug("ES response {}", responseDelete3.get().getResponseBody(StandardCharsets.UTF_8));

        Thread.sleep(2000);
    }

    @Test
    public void testQuery() throws AnalyticsException, IOException {
        
    	//Do the call
        final MonitoringResponse monitoringResponse = elasticMonitoringRepository.query("1876c024-c6a2-409a-b6c0-24c6a2e09a5f");

        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        logger.info("ES reponse: {}", mapper.writeValueAsString(monitoringResponse));
        
        // assert
        final String expectedJson = this.freeMarkerComponent.generateFromTemplate("monitoringExpectedResponse.json");
        final MonitoringResponse expectedResponse = mapper.readValue(expectedJson, MonitoringResponse.class);
        expectedResponse.setTimestamp(monitoringResponse.getTimestamp());

        //FIXME need to create an equals method in MonitoringResponse in order to be able to use Assert.assertEquals
        ReflectionAssert.assertReflectionEquals(expectedResponse, monitoringResponse);
    }
}
