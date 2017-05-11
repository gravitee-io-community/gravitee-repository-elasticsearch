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
import io.gravitee.repository.analytics.query.AggregationType;
import io.gravitee.repository.analytics.query.Order;
import io.gravitee.repository.analytics.query.SortBuilder;
import io.gravitee.repository.analytics.query.SortType;
import io.gravitee.repository.analytics.query.count.CountResponse;
import io.gravitee.repository.analytics.query.groupby.GroupByResponse;
import io.gravitee.repository.analytics.query.response.histogram.DateHistogramResponse;
import io.gravitee.repository.elasticsearch.spring.AnalyticsRepositoryTestConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import static io.gravitee.repository.analytics.query.DateRangeBuilder.lastDays;
import static io.gravitee.repository.analytics.query.IntervalBuilder.hours;
import static io.gravitee.repository.analytics.query.QueryBuilders.*;

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
public class ElasticAnalyticsRepositoryTest {

    @Autowired
    private AnalyticsRepository analyticsRepository;

    @Test
    public void testDateHistogram() throws Exception {
        DateHistogramResponse response = analyticsRepository.query(
                dateHistogram()
                        .timeRange(lastDays(30), hours(1))
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testDateHistogram_root() throws Exception {
        DateHistogramResponse response = analyticsRepository.query(
                dateHistogram()
                        .timeRange(lastDays(30), hours(1))
                        .root("api", "4e0db366-f772-4489-8db3-66f772b48989")
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testDateHistogram_singleAggregation() throws Exception {
        DateHistogramResponse response = analyticsRepository.query(
                dateHistogram()
                        .timeRange(lastDays(30), hours(1))
                        .aggregation(AggregationType.AVG, "response-time")
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testDateHistogram_multipleAggregation_average() throws Exception {
        DateHistogramResponse response = analyticsRepository.query(
                dateHistogram()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .aggregation(AggregationType.AVG, "response-time")
                        .aggregation(AggregationType.AVG, "api-response-time")
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testDateHistogram_multipleAggregation_averageAndField() throws Exception {
        DateHistogramResponse response = analyticsRepository.query(
                dateHistogram()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .aggregation(AggregationType.AVG, "response-time")
                        .aggregation(AggregationType.FIELD, "application")
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testGroupBy_simpleField() throws Exception {
        GroupByResponse response = analyticsRepository.query(
                groupBy()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .field("application")
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testGroupBy_simpleField_withOrder() throws Exception {
        GroupByResponse response = analyticsRepository.query(
                groupBy()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .field("application")
                        .sort(SortBuilder.on("response-time", Order.DESC, SortType.AVG))
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testGroupBy_simpleField_withRanges() throws Exception {
        GroupByResponse response = analyticsRepository.query(
                groupBy()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .field("status")
                        .range(100, 199)
                        .range(200, 299)
                        .range(300, 399)
                        .build());

        Assert.assertNotNull(response);
    }

    @Test
    public void testCount() throws Exception {
        CountResponse response = analyticsRepository.query(
                count()
                        .timeRange(lastDays(30), hours(1))
                        .query("api:4e0db366-f772-4489-8db3-66f772b48989")
                        .build());

        Assert.assertNotNull(response);
    }
}
