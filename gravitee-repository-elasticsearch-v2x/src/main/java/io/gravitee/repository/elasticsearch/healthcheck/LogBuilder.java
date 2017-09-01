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

import io.gravitee.common.http.HttpMethod;
import io.gravitee.repository.healthcheck.query.log.Log;
import io.gravitee.repository.healthcheck.query.log.Step;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
final class LogBuilder {

    /** Document simple date format **/
    private static DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    private final static String FIELD_ID = "id";
    private final static String FIELD_TIMESTAMP = "@timestamp";

    private final static String FIELD_GATEWAY = "gateway";
    private final static String FIELD_ENDPOINT = "endpoint";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_AVAILABLE = "available";
    private final static String FIELD_SUCCESS = "success";
    private final static String FIELD_STATE = "state";

    private final static String FIELD_STEPS = "steps";
    private final static String FIELD_METHOD = "method";
    private final static String FIELD_URL = "url";
    private final static String FIELD_STATUS = "status";
    private final static String FIELD_MESSAGE = "message";

    static Log build(Map<String, Object> source) {
        Log log = new Log();

        log.setId((String) source.get(FIELD_ID));
        log.setTimestamp(dtf.parseDateTime((String) source.get(FIELD_TIMESTAMP)).toInstant().getMillis());
        log.setGateway((String) source.get(FIELD_GATEWAY));
        log.setEndpoint((String) source.get(FIELD_ENDPOINT));
        log.setResponseTime((int) source.get(FIELD_RESPONSE_TIME));
        log.setAvailable((boolean) source.get(FIELD_AVAILABLE));
        log.setState((int) source.get(FIELD_STATE));
        log.setSuccess((boolean) source.get(FIELD_SUCCESS));

        List<Map<String, Object>> steps = (List) source.get(FIELD_STEPS);
        if (steps != null) {
            log.setSteps(
            steps.stream().map(stepMap -> {
                Step step = new Step();
                step.setMessage((String) stepMap.get(FIELD_MESSAGE));
                step.setMethod(HttpMethod.valueOf(((String)stepMap.get(FIELD_METHOD)).toUpperCase()));
                step.setUrl((String) stepMap.get(FIELD_URL));
                step.setStatusCode((int) stepMap.get(FIELD_STATUS));
                step.setSuccess((boolean) stepMap.get(FIELD_SUCCESS));
                return step;
            }).collect(Collectors.toList()));
        }

        return log;
    }
}
