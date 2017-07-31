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
package io.gravitee.repository.elasticsearch.utils;

import java.time.LocalDate;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;

/**
 * Utility Spring been used to compute the elasticsearch index name. 
 * 
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 *
 */
@Component
public class ElasticsearchIndexUtil {
	
	/**
     * Elasticsearch configuration.
     */
    @Autowired
    private ElasticConfiguration configuration;
    
    
    /**
     * Return the list of ES index names separated by a comma
     * @param from start date for the search
     * @param to end date for the search
     * @return the list of ES index names separated by a comma
     */
    public String getIndexName(long from, long to) {
        return DateUtils.rangedIndices(from, to)
                .stream()
                .map(date -> configuration.getIndexName() + '-' + date)
                .collect(Collectors.joining(","));
    }

    /**
     * Get the index name for today.
     * Format is <prefixIndexName>-<yyyy.MM.dd>
     * @return index name for today
     */
    public String getTodayIndexName() {
        final String suffixDay = LocalDate.now().format(DateUtils.ES_DAILY_INDICE);
        return configuration.getIndexName() + '-' + suffixDay;
    }

    /**
     * Get the all indexes name for gravitee.
     * Format is <prefixIndexName>-*
     * @return all indexes name for gravitee
     */
    public String getAllIndexName() {
        return configuration.getIndexName() + "-*";
    }
}
