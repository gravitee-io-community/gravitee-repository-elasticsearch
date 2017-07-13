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

import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.utils.DateUtils;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 * @author GraviteeSource Team
 */
public abstract class AbstractElasticRepository {

    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    /**
     * Elasticsearch configuration.
     */
    @Autowired
    protected ElasticConfiguration configuration;

    /**
     * Elasticsearch component to perform HTTP request.
     */
    @Autowired
    protected ElasticsearchComponent elasticsearchComponent;

    /**
     * Templating component
     */
    @Autowired
    protected FreeMarkerComponent freeMarkerComponent;

    /**
     * Return the list of ES index names separated by a comma
     * @param from start date for the search
     * @param to end date for the search
     * @return the list of ES index names separated by a comma
     */
    protected String getIndexName(long from, long to) {
        return DateUtils.rangedIndices(from, to)
                .stream()
                .map(date -> configuration.getIndexName() + '-' + date)
                .collect(Collectors.joining(","));
    }

    protected String getIndexName() {
        final String suffixDay = LocalDate.now().format(DATE_TIME_FORMATTER);
        return configuration.getIndexName() + '-' + suffixDay;
    }
}
