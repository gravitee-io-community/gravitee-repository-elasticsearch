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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.healthcheck.ElasticHealthCheckRepository;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryConfiguration;
import io.gravitee.repository.elasticsearch.spring.mock.ConfigurationTest;
import io.gravitee.repository.elasticsearch.spring.mock.PropertySourceRepositoryInitializer;
import io.gravitee.repository.healthcheck.HealthResponse;

/**
 * @author Guillaume Waignier (Zenika)
 * @author Sebastien Devaux (Zenika)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {AnalyticsRepositoryConfiguration.class,ConfigurationTest.class},
        initializers = PropertySourceRepositoryInitializer.class)
public class ElasticHealthCheckRepositoryTest {
    
    @Autowired
    private ElasticHealthCheckRepository elasticHealthCheckRepository;

    @Test
    public void testQuery() throws AnalyticsException, IOException {
    	

    	//Do the call
		final Instant now = Instant.now().truncatedTo(ChronoUnit.DAYS);
		final Instant tomorrow = now.plus(1, ChronoUnit.DAYS);
		final Instant yesterday = now.minus(1, ChronoUnit.DAYS);
		
        final HealthResponse healthResponse = elasticHealthCheckRepository.query("bf19088c-f2c7-4fec-9908-8cf2c75fece4", 60, yesterday.toEpochMilli(), tomorrow.toEpochMilli());
        
        
        // create the expected
        //FIXME: need getter/setter to be able to use jackson to create the expected object in one line
        final int length = 49;
        final HealthResponse expected = new HealthResponse();
        final long[] timestamps = new long[length];
        for (int idx = 0 ; idx < length ; ++idx) {
        	timestamps[idx] = yesterday.toEpochMilli() + 3600000L*idx;
        }
        expected.timestamps(timestamps);
        
        final Map<Boolean,long[]> bucket = new HashMap<>();
        expected.buckets(bucket);
        final long[] successState = new long[length];
        final long[] failedState = new long[length];
        bucket.put(true, successState);
        bucket.put(false, failedState);
        successState[17]=5;
        successState[39]=1;
        successState[40]=1;
        successState[41]=3;
        
        // assert
        //FIXME: need equals in healthResponse
        Assert.assertArrayEquals(expected.timestamps(), healthResponse.timestamps());
        Assert.assertArrayEquals(expected.buckets().get(true), healthResponse.buckets().get(true));
        Assert.assertArrayEquals(expected.buckets().get(false), healthResponse.buckets().get(false));
    }
}
