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

import org.springframework.beans.factory.annotation.Autowired;

import io.gravitee.repository.elasticsearch.utils.ElasticsearchIndexUtil;
import io.gravitee.repository.elasticsearch.utils.FreeMarkerComponent;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Guillaume Waignier (zenika)
 * @author Sebastien Devaux (zenika)
 * @author GraviteeSource Team
 */
public abstract class AbstractElasticRepository {

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
     * Util component used to compute index name.
     */
    @Autowired
    protected ElasticsearchIndexUtil elasticsearchIndexUtil;
}
