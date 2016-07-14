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

import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.query.HitsByApiQuery;
import io.gravitee.repository.analytics.query.response.histogram.HistogramResponse;
import io.gravitee.repository.elasticsearch.analytics.spring.AnalyticsRepositoryConfiguration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static io.gravitee.repository.analytics.query.DateRangeBuilder.lastDays;
import static io.gravitee.repository.analytics.query.IntervalBuilder.hours;
import static io.gravitee.repository.analytics.query.QueryBuilders.query;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { AnalyticsRepositoryConfiguration.class })
public class ElasticAnalyticsRepositoryTest {

    @Autowired
    private AnalyticsRepository analyticsRepository;

    @Test
    @Ignore
    public void test() throws Exception {
        Assert.assertNotNull(analyticsRepository);

        HistogramResponse response3 = analyticsRepository.query(query().hitsByApi().period(lastDays(30)).interval(hours(1)).type(HitsByApiQuery.Type.HITS_BY_APPLICATION).build());

        Assert.assertNotNull(response3);
    }

    @Test
    @Ignore
    public void health() throws Exception {
        analyticsRepository.query("api-weather", 1000 * 60 * 5, 1448781600000l, 1448781600000l);
    }
}
