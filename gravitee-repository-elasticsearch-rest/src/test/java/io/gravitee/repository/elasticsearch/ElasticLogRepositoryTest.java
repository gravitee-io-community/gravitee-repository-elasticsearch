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
import io.gravitee.repository.elasticsearch.log.ElasticLogRepository;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryConfiguration;
import io.gravitee.repository.elasticsearch.spring.mock.ConfigurationTest;
import io.gravitee.repository.elasticsearch.spring.mock.PropertySourceRepositoryInitializer;
import io.gravitee.repository.log.model.Request;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static io.gravitee.repository.analytics.query.DateRangeBuilder.lastDays;
import static io.gravitee.repository.analytics.query.IntervalBuilder.hours;
import static io.gravitee.repository.analytics.query.QueryBuilders.tabular;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 * FIXME need to test with equals
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {AnalyticsRepositoryConfiguration.class,ConfigurationTest.class},
        initializers = PropertySourceRepositoryInitializer.class)
public class ElasticLogRepositoryTest {

    @Autowired
    private ElasticLogRepository logRepository;

    @Test
    public void testFindById() throws Exception {
        Request request = logRepository.findById("29381bce-df59-47b2-b81b-cedf59c7b23e");

        Assert.assertNotNull(request);
    }

    @Test
    public void testTabular() throws Exception {
        TabularResponse response = logRepository.query(
                tabular()
                        .timeRange(lastDays(60), hours(1))
                        .query("api:be0aa9c9-ca1c-4d0a-8aa9-c9ca1c5d0aab")
                        .page(1)
                        .size(20)
                        .build());

        Assert.assertNotNull(response);
    }
}
