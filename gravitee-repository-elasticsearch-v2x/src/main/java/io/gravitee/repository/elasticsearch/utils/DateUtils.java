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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public final class DateUtils {

    private final static DateTimeFormatter ES_DAILY_INDICE = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private DateUtils() {}

    public static List<String> rangedIndices(long from, long to) {
        List<String> indices = new ArrayList<>();

        LocalDate start = new Date(from).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate stop = new Date(to).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        while(start.isBefore(stop) || start.isEqual(stop)) {
            indices.add(ES_DAILY_INDICE.format(start));
            start = start.plus(1, ChronoUnit.DAYS);
        }

        return indices;
    }
}
