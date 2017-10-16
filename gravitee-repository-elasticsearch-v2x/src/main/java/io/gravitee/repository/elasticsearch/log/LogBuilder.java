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

import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.repository.log.model.ExtendedLog;
import io.gravitee.repository.log.model.Log;
import io.gravitee.repository.log.model.Request;
import io.gravitee.repository.log.model.Response;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
final class LogBuilder {

    /** Document simple date format **/
    private static DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    private final static String FIELD_REQUEST_ID = "id";
    private final static String FIELD_TRANSACTION_ID = "transaction";
    private final static String FIELD_TIMESTAMP = "@timestamp";
    private final static String FIELD_GATEWAY = "gateway";

    private final static String FIELD_METHOD = "method";
    private final static String FIELD_URI = "uri";
    private final static String FIELD_ENDPOINT = "endpoint";
    private final static String FIELD_REQUEST_CONTENT_LENGTH = "request-content-length";
    private final static String FIELD_RESPONSE_CONTENT_LENGTH = "response-content-length";
    private final static String FIELD_CLIENT_REQUEST = "client-request";
    private final static String FIELD_PROXY_REQUEST = "proxy-request";
    private final static String FIELD_CLIENT_RESPONSE = "client-response";
    private final static String FIELD_PROXY_RESPONSE = "proxy-response";
    private final static String FIELD_BODY = "body";
    private final static String FIELD_HEADERS = "headers";
    private final static String FIELD_STATUS = "status";
    private final static String FIELD_RESPONSE_TIME = "response-time";
    private final static String FIELD_API_RESPONSE_TIME = "api-response-time";

    private final static String FIELD_LOCAL_ADDRESS = "local-address";
    private final static String FIELD_REMOTE_ADDRESS = "remote-address";

    private final static String FIELD_TENANT = "tenant";
    private final static String FIELD_APPLICATION = "application";
    private final static String FIELD_API = "api";
    private final static String FIELD_PLAN = "plan";
    private final static String FIELD_API_KEY = "api-key";

    private final static String FIELD_MESSAGE = "message";

    static Log createLog(Map<String, Object> source) {
        return createLog(source, new Log());
    }

    static ExtendedLog createExtendedLog(Map<String, Object> metrics, Map<String, Object> log) {
        ExtendedLog extentedLog = createLog(metrics, new ExtendedLog());

        // Add client and proxy requests / responses
        if (log != null) {
            Map<String, Object> clientRequest = (Map<String, Object>) log.get(FIELD_CLIENT_REQUEST);
            extentedLog.setClientRequest(createRequest(clientRequest));
            Map<String, Object> proxyRequest = (Map<String, Object>) log.get(FIELD_PROXY_REQUEST);
            extentedLog.setProxyRequest(createRequest(proxyRequest));
            Map<String, Object> clientResponse = (Map<String, Object>) log.get(FIELD_CLIENT_RESPONSE);
            extentedLog.setClientResponse(createResponse(clientResponse));
            Map<String, Object> proxyResponse = (Map<String, Object>) log.get(FIELD_PROXY_RESPONSE);
            extentedLog.setProxyResponse(createResponse(proxyResponse));
        }

        return extentedLog;
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

    private static <T extends Log> T createLog(Map<String, Object> source, T log) {
        log.setId((String) source.get(FIELD_REQUEST_ID));
        log.setTransactionId((String) source.get(FIELD_TRANSACTION_ID));
        log.setTimestamp(dtf.parseDateTime((String) source.get(FIELD_TIMESTAMP)).toInstant().getMillis());

        log.setUri((String) source.get(FIELD_URI));
        Object method = source.get(FIELD_METHOD);
        if (method instanceof Integer) {
            log.setMethod(HttpMethod.get((int) method));
        } else {
            log.setMethod(HttpMethod.valueOf((String) method));
        }
        log.setEndpoint((String) source.get(FIELD_ENDPOINT));

        log.setStatus((int) source.get(FIELD_STATUS));
        log.setResponseTime((int) source.get(FIELD_RESPONSE_TIME));

        Integer apiResponseTime = (Integer) source.get(FIELD_API_RESPONSE_TIME);
        if (apiResponseTime != null) {
            log.setApiResponseTime(apiResponseTime);
        }

        Integer requestContentLength = (Integer) source.get(FIELD_REQUEST_CONTENT_LENGTH);
        if (requestContentLength != null) {
            log.setRequestContentLength(requestContentLength);
        }

        Integer responseContentLength = (Integer) source.get(FIELD_RESPONSE_CONTENT_LENGTH);
        if (responseContentLength != null) {
            log.setResponseContentLength(responseContentLength);
        }

        log.setLocalAddress((String) source.get(FIELD_LOCAL_ADDRESS));
        log.setRemoteAddress((String) source.get(FIELD_REMOTE_ADDRESS));

        log.setTenant((String) source.get(FIELD_TENANT));
        log.setApplication((String) source.get(FIELD_APPLICATION));
        log.setApi((String) source.get(FIELD_API));
        log.setPlan((String) source.get(FIELD_PLAN));
        log.setApiKey((String) source.get(FIELD_API_KEY));
        log.setGateway((String) source.get(FIELD_GATEWAY));
        log.setMessage((String) source.get(FIELD_MESSAGE));

        return log;
    }
}
