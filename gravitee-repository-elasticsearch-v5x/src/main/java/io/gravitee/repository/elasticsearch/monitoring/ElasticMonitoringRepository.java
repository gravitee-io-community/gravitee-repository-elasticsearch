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
package io.gravitee.repository.elasticsearch.monitoring;

import io.gravitee.repository.elasticsearch.AbstractElasticRepository;
import io.gravitee.repository.elasticsearch.utils.DateUtils;
import io.gravitee.repository.monitoring.MonitoringRepository;
import io.gravitee.repository.monitoring.model.MonitoringResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Map;

import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * @author Azize Elamrani (azize dot elamrani at gmail dot com)
 * @author GraviteeSource Team
 */
public class ElasticMonitoringRepository extends AbstractElasticRepository implements MonitoringRepository {

    private final static String FIELD_GATEWAY_NAME = "gateway";
    private final static String FIELD_TIMESTAMP = "@timestamp";
    private final static String FIELD_HOSTNAME = "hostname";

    private final static String FIELD_JVM = "jvm";
    private final static String FIELD_PROCESS = "process";
    private final static String FIELD_OS = "os";

    private final static String TYPE_MONITOR = "monitor";

    @Autowired
    private Client client;

    @Override
    public MonitoringResponse query(final String gatewayId) {
        final String suffixDay = LocalDate.now().format(DateUtils.ES_DAILY_INDICE);

        final SearchRequestBuilder monitor = client
                .prepareSearch(configuration.getIndexName() + '-' + suffixDay)
                .setTypes(TYPE_MONITOR)
                .setQuery(boolQuery().must(termQuery(FIELD_GATEWAY_NAME, gatewayId)))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addSort(FIELD_TIMESTAMP, SortOrder.DESC)
                .setSize(1);

        final SearchResponse searchResponse = monitor.get();
        final SearchHits hits = searchResponse.getHits();
        if (hits != null && hits.getHits().length > 0) {
            return convert(hits.getHits()[0].getSource());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private MonitoringResponse convert(final Map<String, Object> source) {
        final MonitoringResponse monitoringResponse = new MonitoringResponse();
        monitoringResponse.setGatewayId((String) source.get(FIELD_GATEWAY_NAME));
        monitoringResponse.setTimestamp(ZonedDateTime.parse((String) source.get(FIELD_TIMESTAMP)));
        monitoringResponse.setHostname((String) source.get(FIELD_HOSTNAME));

        // OS

        final Map<String, Object> os = (Map<String, Object>) source.get(FIELD_OS);

        final Map<String, Object> cpu = (Map<String, Object>) os.get("cpu");
        monitoringResponse.setOsCPUPercent((Integer) cpu.get("percent"));
        monitoringResponse.setOsCPULoadAverage((Map<String, ? super Number  >) cpu.get("load_average"));

        final Map<String, Object> osMem = (Map<String, Object>) os.get("mem");
        monitoringResponse.setOsMemUsedInBytes(getLongValue(osMem.get("used_in_bytes")));
        monitoringResponse.setOsMemFreeInBytes(getLongValue(osMem.get("free_in_bytes")));
        monitoringResponse.setOsMemTotalInBytes(getLongValue(osMem.get("total_in_bytes")));
        monitoringResponse.setOsMemUsedPercent((Integer) osMem.get("used_percent"));
        monitoringResponse.setOsMemFreePercent((Integer) osMem.get("free_percent"));

        // Process

        final Map<String, Object> process = (Map<String, Object>) source.get(FIELD_PROCESS);

        monitoringResponse.setJvmProcessOpenFileDescriptors((Integer) process.get("open_file_descriptors"));
        monitoringResponse.setJvmProcessMaxFileDescriptors((Integer) process.get("max_file_descriptors"));

        // JVM

        final Map<String, Object> jvm = (Map<String, Object>) source.get(FIELD_JVM);

        monitoringResponse.setJvmUptimeInMillis(getLongValue(jvm.get("uptime_in_millis")));
        monitoringResponse.setJvmTimestamp(getLongValue(jvm.get("timestamp")));

        final Map<String, Object> jvmMem = (Map<String, Object>) jvm.get("mem");
        monitoringResponse.setJvmHeapCommittedInBytes(getLongValue(jvmMem.get("heap_committed_in_bytes")));
        monitoringResponse.setJvmHeapUsedPercent((Integer) jvmMem.get("heap_used_percent"));
        monitoringResponse.setJvmHeapMaxInBytes(getLongValue(jvmMem.get("heap_max_in_bytes")));
        monitoringResponse.setJvmNonHeapCommittedInBytes(getLongValue(jvmMem.get("non_heap_committed_in_bytes")));
        monitoringResponse.setJvmHeapUsedInBytes(getLongValue(jvmMem.get("heap_used_in_bytes")));
        monitoringResponse.setJvmNonHeapUsedInBytes(getLongValue(jvmMem.get("non_heap_used_in_bytes")));

        final Map<String, Object> jvmMemPools = (Map<String, Object>) jvmMem.get("pools");

        final Map<String, Object> jvmMemPoolsYoung = (Map<String, Object>) jvmMemPools.get("young");

        monitoringResponse.setJvmMemPoolYoungUsedInBytes(getLongValue(jvmMemPoolsYoung.get("used_in_bytes")));
        monitoringResponse.setJvmMemPoolYoungPeakUsedInBytes(getLongValue(jvmMemPoolsYoung.get("peak_used_in_bytes")));
        monitoringResponse.setJvmMemPoolYoungMaxInBytes(getLongValue(jvmMemPoolsYoung.get("max_in_bytes")));
        monitoringResponse.setJvmMemPoolYoungPeakMaxInBytes(getLongValue(jvmMemPoolsYoung.get("peak_max_in_bytes")));

        final Map<String, Object> jvmMemPoolsOld = (Map<String, Object>) jvmMemPools.get("old");

        monitoringResponse.setJvmMemPoolOldUsedInBytes(getLongValue(jvmMemPoolsOld.get("used_in_bytes")));
        monitoringResponse.setJvmMemPoolOldPeakUsedInBytes(getLongValue(jvmMemPoolsOld.get("peak_used_in_bytes")));
        monitoringResponse.setJvmMemPoolOldMaxInBytes(getLongValue(jvmMemPoolsOld.get("max_in_bytes")));
        monitoringResponse.setJvmMemPoolOldPeakMaxInBytes(getLongValue(jvmMemPoolsOld.get("peak_max_in_bytes")));

        final Map<String, Object> jvmMemPoolsSurvivor = (Map<String, Object>) jvmMemPools.get("survivor");

        monitoringResponse.setJvmMemPoolSurvivorUsedInBytes(getLongValue(jvmMemPoolsSurvivor.get("used_in_bytes")));
        monitoringResponse.setJvmMemPoolSurvivorPeakUsedInBytes(getLongValue(jvmMemPoolsSurvivor.get("peak_used_in_bytes")));
        monitoringResponse.setJvmMemPoolSurvivorMaxInBytes(getLongValue(jvmMemPoolsSurvivor.get("max_in_bytes")));
        monitoringResponse.setJvmMemPoolSurvivorPeakMaxInBytes(getLongValue(jvmMemPoolsSurvivor.get("peak_max_in_bytes")));

        final Map<String, Object> jvmThreads = (Map<String, Object>) jvm.get("threads");

        monitoringResponse.setJvmThreadCount((Integer) jvmThreads.get("count"));
        monitoringResponse.setJvmThreadPeakCount((Integer) jvmThreads.get("peak_count"));

        final Map<String, Object> jvmGC = (Map<String, Object>) jvm.get("gc");

        final Map<String, Object> jvmGCCollectors = (Map<String, Object>) jvmGC.get("collectors");

        final Map<String, Object> jvmGCCollectorsYoung = (Map<String, Object>) jvmGCCollectors.get("young");

        monitoringResponse.setJvmGCCollectorsYoungCollectionCount((Integer) jvmGCCollectorsYoung.get("collection_count"));
        monitoringResponse.setJvmGCCollectorsYoungCollectionTimeInMillis(getLongValue(jvmGCCollectorsYoung.get("collection_time_in_millis")));

        final Map<String, Object> jvmGCCollectorsOld = (Map<String, Object>) jvmGCCollectors.get("old");

        monitoringResponse.setJvmGCCollectorsOldCollectionCount((Integer) jvmGCCollectorsOld.get("collection_count"));
        monitoringResponse.setJvmGCCollectorsOldCollectionTimeInMillis(getLongValue(jvmGCCollectorsOld.get("collection_time_in_millis")));

        return monitoringResponse;
    }

    private Long getLongValue(final Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Long) {
            return (Long) value;
        } else {
            throw new IllegalArgumentException(format("Expected a Long and get a %s", value.getClass().getName()));
        }
    }
}
