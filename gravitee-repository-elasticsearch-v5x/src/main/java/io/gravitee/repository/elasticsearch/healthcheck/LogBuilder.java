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

import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.repository.healthcheck.query.log.*;
import org.elasticsearch.search.SearchHit;
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

    private final static String FIELD_TIMESTAMP = "@timestamp";

    private final static String FIELD_GATEWAY = "gateway";
    private final static String FIELD_ENDPOINT = "endpoint";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_AVAILABLE = "available";
    private final static String FIELD_SUCCESS = "success";
    private final static String FIELD_STATE = "state";

    private final static String FIELD_STEPS = "steps";
    private final static String FIELD_METHOD = "method";
    private final static String FIELD_URI = "uri";
    private final static String FIELD_STATUS = "status";
    private final static String FIELD_MESSAGE = "message";

    private final static String FIELD_BODY = "body";
    private final static String FIELD_HEADERS = "headers";
    private final static String FIELD_RESPONSE = "response";
    private final static String FIELD_REQUEST = "request";

    static Log createLog(SearchHit searchHit) {
        Log log = new Log();

        Map<String, Object> source = searchHit.getSource();
        log.setId(searchHit.getId());
        log.setTimestamp(dtf.parseDateTime((String) source.get(FIELD_TIMESTAMP)).toInstant().getMillis());
        log.setGateway((String) source.get(FIELD_GATEWAY));
        log.setEndpoint((String) source.get(FIELD_ENDPOINT));
        log.setResponseTime((int) source.get(FIELD_RESPONSE_TIME));
        log.setAvailable((boolean) source.get(FIELD_AVAILABLE));
        log.setState((int) source.get(FIELD_STATE));
        log.setSuccess((boolean) source.get(FIELD_SUCCESS));

        List<Map<String, Object>> steps = (List) source.get(FIELD_STEPS);
        if (steps != null && ! steps.isEmpty()) {

            Map<String, Object> stepMap = steps.iterator().next();
            Map<String, Object> requestMap = (Map<String, Object>) stepMap.get(FIELD_REQUEST);
            Map<String, Object> responseMap = (Map<String, Object>) stepMap.get(FIELD_RESPONSE);

            if (requestMap != null) {
                log.setUri((String) requestMap.get(FIELD_URI));
                log.setMethod(HttpMethod.valueOf(((String) requestMap.get(FIELD_METHOD)).toUpperCase()));
                log.setStatus((int) responseMap.get(FIELD_STATUS));
            } else {
                // Ensure backward compatibility
                log.setUri((String) stepMap.get(FIELD_URI));
                log.setMethod(HttpMethod.valueOf(((String) stepMap.get(FIELD_METHOD)).toUpperCase()));
                log.setStatus((int) stepMap.get(FIELD_STATUS));
            }
        }

        return log;
    }

    static ExtendedLog createExtendedLog(SearchHit searchHit) {
        ExtendedLog log = new ExtendedLog();

        Map<String, Object> source = searchHit.getSource();
        log.setId(searchHit.getId());
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
                        step.setSuccess((boolean) stepMap.get(FIELD_SUCCESS));
                        step.setMessage((String) stepMap.get(FIELD_MESSAGE));

                        Map<String, Object> requestMap = (Map<String, Object>) stepMap.get(FIELD_REQUEST);
                        Map<String, Object> responseMap = (Map<String, Object>) stepMap.get(FIELD_RESPONSE);

                        if (requestMap != null) {
                            step.setRequest(createRequest(requestMap));
                            step.setResponse(createResponse(responseMap));
                        } else {
                            // Ensure backward compatibility
                            Request request = new Request();
                            request.setUri((String) stepMap.get(FIELD_URI));
                            request.setMethod(HttpMethod.valueOf(((String) stepMap.get(FIELD_METHOD)).toUpperCase()));
                            step.setRequest(request);

                            Response response = new Response();
                            response.setStatus((int) stepMap.get(FIELD_STATUS));
                            step.setResponse(response);
                        }

                        return step;
                    }).collect(Collectors.toList()));
        }

        return log;
    }

    private static Request createRequest(Map<String, Object> source) {
        if (source == null) {
            return null;
        }

        Request request = new Request();
        request.setUri((String) source.get(FIELD_URI));
        request.setMethod(HttpMethod.valueOf((String) source.get(FIELD_METHOD)));
        request.setBody((String) source.get(FIELD_BODY));
        request.setHeaders(createHttpHeaders((Map<String, List<String>>) source.get(FIELD_HEADERS)));
        return request;
    }

    private static Response createResponse(Map<String, Object> source) {
        if (source == null) {
            return null;
        }

        Response response = new Response();
        response.setStatus((int) source.get(FIELD_STATUS));
        response.setBody((String) source.get(FIELD_BODY));
        response.setHeaders(createHttpHeaders((Map<String, List<String>>) source.get(FIELD_HEADERS)));
        return response;
    }

    private static HttpHeaders createHttpHeaders(Map<String, List<String>> headers) {
        if (headers == null) {
            return null;
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        headers.forEach(httpHeaders::put);
        return httpHeaders;
    }
}
