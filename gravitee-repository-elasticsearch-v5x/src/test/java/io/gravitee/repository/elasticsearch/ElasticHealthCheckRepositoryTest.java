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

import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryTestConfiguration;
import io.gravitee.repository.healthcheck.api.HealthCheckRepository;
import io.gravitee.repository.healthcheck.query.QueryBuilders;
import io.gravitee.repository.healthcheck.query.availability.AvailabilityResponse;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners(
        listeners = { ElasticsearchNodeListener.class,
                DependencyInjectionTestExecutionListener.class })
@ContextConfiguration(
        classes = AnalyticsRepositoryTestConfiguration.class,
        initializers = PropertySourceRepositoryInitializer.class)
@Ignore
public class ElasticHealthCheckRepositoryTest {

    @Autowired
    private HealthCheckRepository healthCheckRepository;

    @Test
    public void testHealthCheck() throws Exception {
        Assert.assertNotNull(healthCheckRepository);

        AvailabilityResponse availabilityResponse = healthCheckRepository.query(
                QueryBuilders.availability().api("4e0db366-f772-4489-8db3-66f772b48989").build()
        );

        Assert.assertNotNull(availabilityResponse);
    }
}
