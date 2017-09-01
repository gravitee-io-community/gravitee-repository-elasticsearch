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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.io.File;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Configuration
public class AnalyticsRepositoryTestConfiguration extends AnalyticsRepositoryConfiguration {

    @Bean
    @Override
    public ElasticClientTestFactory elasticClientFactory() {
        return new ElasticClientTestFactory();
    }

    @Bean
    public Node elasticsearchNode() {
        Settings settings = Settings.builder()
                .put("path.home", System.getProperty("java.io.tmpdir"))
                .put("transport.type", "local")
                .put("http.enabled","false")
                .build();
        return new Node(settings);
    }
}
