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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public abstract class AbstractElasticRepository {

    protected final static String FIELD_TIMESTAMP = "@timestamp";

    @Autowired
    protected ElasticConfiguration configuration;

    @Autowired
    protected Client client;

    protected SearchRequestBuilder createRequest(String type, long from, long to) {
        String [] rangedIndices = DateUtils.rangedIndices(from, to)
                .stream()
                .map(date -> configuration.getIndexName() + '-' + date)
                .toArray(String[]::new);

        return client
                .prepareSearch(rangedIndices)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setTypes(type)
                .setSize(0);
    }
}
