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
package io.gravitee.repository.elasticsearch.healthcheck;

import com.fasterxml.jackson.databind.JsonNode;
import io.gravitee.repository.analytics.AnalyticsException;
import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.elasticsearch.model.elasticsearch.Aggregation;
import io.gravitee.repository.elasticsearch.model.elasticsearch.ESSearchResponse;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.healthcheck.HealthCheckRepository;
import io.gravitee.repository.healthcheck.HealthResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 * @author GraviteeSource Team
 */
public class ElasticHealthCheckRepository extends AbstractElasticRepository implements HealthCheckRepository {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(ElasticHealthCheckRepository.class);

    private final static String TYPE_HEALTH = "/health";

    private final static String HEALTHCHECK_TEMPLATE = "healthCheckRequest.ftl";

    @Override
    public HealthResponse query(final String api, final long interval, final long from, final long to) throws AnalyticsException {

    	try {
            final Map<String, Object> datas = new HashMap<>();
            datas.put("api", api);
            datas.put("interval", interval);
            datas.put("from", from);
            datas.put("to", to);

            final String query = this.freeMarkerComponent.generateFromTemplate(HEALTHCHECK_TEMPLATE, datas);

			final ESSearchResponse result = this.elasticsearchComponent.search(this.getIndexName(from, to) + TYPE_HEALTH, query);
            logger.debug("ES response {}", result);

            return this.toHealthResponse(result);
    	} catch (final TechnicalException e) {
    		logger.error("An error occurs while looking for analytics with Elasticsearch", e);
    		throw new AnalyticsException("An error occurs while looking for analytics with Elasticsearch", e);
    	}
    }


    /**
     * Convert a raw ES response into a HealthResponse
     * @param searchResponse the raw ES response
     * @return the HealthResponse response
     */
    private HealthResponse toHealthResponse(final ESSearchResponse searchResponse) {
        final HealthResponse healthResponse = new HealthResponse();

        if (searchResponse.getAggregations() == null) {
            return healthResponse;
        }

        // First aggregation is always a date histogram aggregation
        final Aggregation histogram = searchResponse.getAggregations().get("by_date");

        // init the response
        this.fillEmptyHealthResponse(healthResponse, histogram.getBuckets().size());

        // Prepare data
        int idx = 0;
        for (final JsonNode bucket : histogram.getBuckets()) {
        	healthResponse.timestamps()[idx] = bucket.get("key").asLong();
            
            final JsonNode subBuckets = bucket.get("by_result").get("buckets");

            for (final JsonNode node : subBuckets) {
            	final boolean isSuccess = node.get("key").asBoolean() == true;
            	healthResponse.buckets().get(isSuccess)[idx] = node.get("doc_count").asInt();
            }
            idx++;
        }

        return healthResponse;
    }
    
    /**
     * Fill an empty healthResponse
     * @param healthResponse the response to fill with empty success and failure
     * @param numberHistogram number of date histogram
     */
    private void fillEmptyHealthResponse(final HealthResponse healthResponse, final int numberHistogram) {
    	final long [] timestamps = new long[numberHistogram];
        final Map<Boolean, long[]> values = new HashMap<>(2);
        values.put(true, new long[timestamps.length]);
        values.put(false, new long[timestamps.length]);
        healthResponse.timestamps(timestamps);
        healthResponse.buckets(values);
    }
}
