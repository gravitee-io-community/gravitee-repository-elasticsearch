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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.monitoring.ElasticMonitoringRepository;
import io.gravitee.repository.elasticsearch.spring.mock.ConfigurationTest;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.monitoring.model.MonitoringResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;

/**
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { ConfigurationTest.class })
public class ElasticMonitoringRepositoryTest {

    /** Logger. */
    private final Logger logger = LoggerFactory.getLogger(ElasticMonitoringRepositoryTest.class);

    @Autowired
    private ElasticMonitoringRepository elasticMonitoringRepository;

    /**
     * Templating tool.
     */
    @Autowired
    private FreeMarkerComponent freeMarkerComponent;

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
