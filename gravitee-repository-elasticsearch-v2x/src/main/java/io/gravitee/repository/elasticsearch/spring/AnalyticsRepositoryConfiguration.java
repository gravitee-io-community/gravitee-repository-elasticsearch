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
import io.gravitee.repository.elasticsearch.analytics.ElasticAnalyticsRepository;
import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.healthcheck.ElasticHealthCheckRepository;
import io.gravitee.repository.elasticsearch.monitoring.ElasticMonitoringRepository;
import io.gravitee.repository.healthcheck.HealthCheckRepository;
import io.gravitee.repository.monitoring.MonitoringRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Configuration
public class AnalyticsRepositoryConfiguration {

    @Bean
    public ElasticClientFactory elasticClientFactory() {
        return new ElasticClientFactory();
    }

    @Bean
    public ElasticConfiguration elasticConfiguration() {
        return new ElasticConfiguration();
    }

    @Bean
    public HealthCheckRepository healthCheckRepository() {
        return new ElasticHealthCheckRepository();
    }

    @Bean
    public AnalyticsRepository analyticsRepository() {
        return new ElasticAnalyticsRepository();
    }

    @Bean
    public MonitoringRepository monitoringRepository() {
        return new ElasticMonitoringRepository();
    }
}
