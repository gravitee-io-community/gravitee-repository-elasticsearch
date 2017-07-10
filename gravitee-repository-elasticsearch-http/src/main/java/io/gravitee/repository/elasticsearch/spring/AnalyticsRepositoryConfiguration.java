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
package io.gravitee.repository.elasticsearch.spring;

import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.elasticsearch.ElasticsearchComponent;
import io.gravitee.repository.elasticsearch.analytics.ElasticAnalyticsRepository;
import io.gravitee.repository.elasticsearch.analytics.query.CountQueryCommand;
import io.gravitee.repository.elasticsearch.analytics.query.DateHistogramQueryCommand;
import io.gravitee.repository.elasticsearch.analytics.query.GroupByQueryCommand;
import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.log.ElasticLogRepository;
import io.gravitee.repository.elasticsearch.monitoring.ElasticMonitoringRepository;
import io.gravitee.repository.elasticsearch.utils.ElasticsearchIndexUtil;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import io.gravitee.repository.monitoring.MonitoringRepository;
import io.vertx.rxjava.core.Vertx;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Guillaume Waignier (zenika)
 * @author Sebastien Devaux (zenika)
 * @author GraviteeSource Team
 */
@Configuration
@Import(
        HealthCheckConfiguration.class
)
public class AnalyticsRepositoryConfiguration {

    @Bean
    public Vertx vertxRx(io.vertx.core.Vertx vertx) {
        return Vertx.newInstance(vertx);
    }

    @Bean
    public ElasticConfiguration elasticConfiguration() {
        return new ElasticConfiguration();
    }

    @Bean
    public AnalyticsRepository analyticsRepository() {
        return new ElasticAnalyticsRepository();
    }
    
    @Bean
    public CountQueryCommand countQueryCommand() {
        return new CountQueryCommand();
    }
    
    @Bean
    public DateHistogramQueryCommand dateHistogramQueryCommand() {
        return new DateHistogramQueryCommand();
    }
    
    @Bean
    public GroupByQueryCommand groupByQueryCommand() {
        return new GroupByQueryCommand();
    }

    @Bean
    public MonitoringRepository monitoringRepository() {
        return new ElasticMonitoringRepository();
    }

    @Bean
    public FreeMarkerComponent freeMarckerComponent() {
        return new FreeMarkerComponent();
    }
    
    @Bean
    public ElasticsearchComponent elasticsearchComponent() {
        return new ElasticsearchComponent();
    }

    @Bean
    public ElasticLogRepository elasticLogRepository() {
    	return new ElasticLogRepository(); 
    }
    
    @Bean
    public ElasticsearchIndexUtil elasticsearchIndexUtil() {
    	return new ElasticsearchIndexUtil(); 
    }
}
