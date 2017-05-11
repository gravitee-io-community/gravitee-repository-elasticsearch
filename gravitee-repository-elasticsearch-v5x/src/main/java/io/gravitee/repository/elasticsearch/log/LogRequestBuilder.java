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
package io.gravitee.repository.elasticsearch.log;

import io.gravitee.common.http.HttpMethod;
import io.gravitee.repository.log.model.Request;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
final class LogRequestBuilder {

    /** Document simple date format **/
    private static DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    private final static String FIELD_REQUEST_ID = "id";
    private final static String FIELD_TRANSACTION_ID = "transaction";
    private final static String FIELD_TIMESTAMP = "@timestamp";

    private final static String FIELD_METHOD = "method";
    private final static String FIELD_URI = "uri";
    private final static String FIELD_PATH = "path";
    private final static String FIELD_ENDPOINT = "endpoint";
    private final static String FIELD_REQUEST_CONTENT_LENGTH = "request-content-length";
    private final static String FIELD_RESPONSE_CONTENT_LENGTH = "response-content-length";
    private final static String FIELD_CLIENT_REQUEST_HEADERS = "client-request-headers";
    private final static String FIELD_CLIENT_RESPONSE_HEADERS = "client-response-headers";
    private final static String FIELD_PROXY_REQUEST_HEADERS = "proxy-request-headers";
    private final static String FIELD_PROXY_RESPONSE_HEADERS = "proxy-response-headers";

    private final static String FIELD_STATUS = "status";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_API_RESPONSE_TIME = "api-response-time";

    private final static String FIELD_LOCAL_ADDRESS = "local-address";
    private final static String FIELD_REMOTE_ADDRESS = "remote-address";

    private final static String FIELD_TENANT = "tenant";
    private final static String FIELD_APPLICATION = "application";
    private final static String FIELD_API = "api";
    private final static String FIELD_PLAN = "plan";
    private final static String FIELD_USER = "user";
    private final static String FIELD_API_KEY = "api-key";

    static Request build(Map<String, Object> source, boolean full) {
        Request request = new Request();

        request.setId((String) source.get(FIELD_REQUEST_ID));
        request.setTransactionId((String) source.get(FIELD_TRANSACTION_ID));
        request.setTimestamp(dtf.parseDateTime((String) source.get(FIELD_TIMESTAMP)).toInstant().getMillis());

        request.setUri((String) source.get(FIELD_URI));
        request.setPath((String) source.get(FIELD_PATH));
        request.setMethod(HttpMethod.valueOf((String) source.get(FIELD_METHOD)));
        request.setEndpoint((String) source.get(FIELD_ENDPOINT));

        request.setStatus((int) source.get(FIELD_STATUS));
        request.setResponseTime((int) source.get(FIELD_RESPONSE_TIME));

        Integer apiResponseTime = (Integer) source.get(FIELD_API_RESPONSE_TIME);
        if (apiResponseTime != null) {
            request.setApiResponseTime(apiResponseTime);
        }

        Integer requestContentLength = (Integer) source.get(FIELD_REQUEST_CONTENT_LENGTH);
        if (requestContentLength != null) {
            request.setRequestContentLength(requestContentLength);
        }

        Integer responseContentLength = (Integer) source.get(FIELD_RESPONSE_CONTENT_LENGTH);
        if (responseContentLength != null) {
            request.setResponseContentLength(responseContentLength);
        }

        request.setLocalAddress((String) source.get(FIELD_LOCAL_ADDRESS));
        request.setRemoteAddress((String) source.get(FIELD_REMOTE_ADDRESS));

        request.setTenant((String) source.get(FIELD_TENANT));
        request.setApplication((String) source.get(FIELD_APPLICATION));
        request.setApi((String) source.get(FIELD_API));
        request.setPlan((String) source.get(FIELD_PLAN));
        request.setUser((String) source.get(FIELD_USER));
        request.setApiKey((String) source.get(FIELD_API_KEY));

        if (full) {
            request.setClientRequestHeaders((HashMap) source.get(FIELD_CLIENT_REQUEST_HEADERS));
            request.setClientResponseHeaders((HashMap) source.get(FIELD_CLIENT_RESPONSE_HEADERS));
            request.setProxyRequestHeaders((HashMap) source.get(FIELD_PROXY_REQUEST_HEADERS));
            request.setProxyResponseHeaders((HashMap) source.get(FIELD_PROXY_RESPONSE_HEADERS));
        }

        return request;
    }
}
