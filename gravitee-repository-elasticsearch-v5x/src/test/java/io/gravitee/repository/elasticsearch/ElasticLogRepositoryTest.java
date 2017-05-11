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

import io.gravitee.repository.analytics.query.tabular.TabularResponse;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryTestConfiguration;
import io.gravitee.repository.log.api.LogRepository;
import io.gravitee.repository.log.model.Request;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import static io.gravitee.repository.analytics.query.DateRangeBuilder.lastDays;
import static io.gravitee.repository.analytics.query.IntervalBuilder.hours;
import static io.gravitee.repository.analytics.query.QueryBuilders.tabular;

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
public class ElasticLogRepositoryTest {

    @Autowired
    private LogRepository logRepository;

    @Test
    public void testFindById() throws Exception {
        Request request = logRepository.findById("eb61a902-6ea6-4d8a-a1a9-026ea64d8a33");

        Assert.assertNotNull(request);
    }

    @Test
    public void testTabular() throws Exception {
        TabularResponse response = logRepository.query(
                tabular()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .page(1)
                        .size(20)
                        .build());

        Assert.assertNotNull(response);
    }
}
