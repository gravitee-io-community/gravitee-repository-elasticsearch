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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.repository.log.model.Request;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
final class LogRequestBuilder {

    /** Document simple date format **/
    private static SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

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

    private final static String FIELD_MESSAGE = "message";

    static Request build(JsonNode source, boolean full) {
        Request request = new Request();

        request.setId((source.get(FIELD_REQUEST_ID).asText()));
        request.setTransactionId(source.get(FIELD_TRANSACTION_ID).asText());

        try {
            request.setTimestamp(dtf.parse((source.get(FIELD_TIMESTAMP).asText())).getTime());
        } catch (ParseException e) {
            // TODO log error
        }

        request.setUri(source.get(FIELD_URI).asText());
        request.setPath(source.get(FIELD_PATH).asText());
        request.setMethod(HttpMethod.valueOf(source.get(FIELD_METHOD).asText()));
        request.setEndpoint(source.get(FIELD_ENDPOINT).asText());

        request.setStatus(source.get(FIELD_STATUS).asInt());
        request.setResponseTime(source.get(FIELD_RESPONSE_TIME).asInt());

        Integer apiResponseTime = source.get(FIELD_API_RESPONSE_TIME).asInt();
        if (apiResponseTime != null) {
            request.setApiResponseTime(apiResponseTime);
        }

        Integer requestContentLength = source.get(FIELD_REQUEST_CONTENT_LENGTH).asInt();
        if (requestContentLength != null) {
            request.setRequestContentLength(requestContentLength);
        }

        Integer responseContentLength = source.get(FIELD_RESPONSE_CONTENT_LENGTH).asInt();
        if (responseContentLength != null) {
            request.setResponseContentLength(responseContentLength);
        }

        request.setLocalAddress(source.get(FIELD_LOCAL_ADDRESS).asText());
        request.setRemoteAddress(source.get(FIELD_REMOTE_ADDRESS).asText());

        request.setTenant(source.get(FIELD_TENANT).asText());
        request.setApplication(source.get(FIELD_APPLICATION).asText());
        request.setApi((source.get(FIELD_API).asText()));
        request.setPlan(source.get(FIELD_PLAN).asText());
        request.setUser(source.get(FIELD_USER).asText());
        request.setApiKey( source.get(FIELD_API_KEY).asText());

        if (full) {
            request.setClientRequestHeaders(convertToMap(source.get(FIELD_CLIENT_REQUEST_HEADERS)));
            request.setClientResponseHeaders(convertToMap(source.get(FIELD_CLIENT_RESPONSE_HEADERS)));
            request.setProxyRequestHeaders(convertToMap(source.get(FIELD_PROXY_REQUEST_HEADERS)));
            request.setProxyResponseHeaders(convertToMap(source.get(FIELD_PROXY_RESPONSE_HEADERS)));

            final JsonNode messageNode = source.get(FIELD_MESSAGE);
            if (messageNode != null) {
                request.setMessage(messageNode.asText());
            }
        }

        return request;
    }

    private static Map<String, List<String>> convertToMap(final JsonNode jsonNode) {
        Map<String, List<String>> result = new HashMap<>();

        final Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()) {
            final String name = iterator.next();
            final ArrayNode values = (ArrayNode) jsonNode.get(name);
            result.put(name, convertToList(values));
        }
        return result;
    }

    private static List<String> convertToList(ArrayNode values) {
        final List<String> result = new ArrayList<>(values.size());
        values.forEach(jsonNode -> result.add(jsonNode.asText()));
        return result;
    }
}
