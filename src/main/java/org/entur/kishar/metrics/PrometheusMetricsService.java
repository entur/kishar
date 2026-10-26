/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

package org.entur.kishar.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PrometheusMetricsService extends PrometheusMeterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsService.class);

    private final String METRICS_PREFIX = "app.kishar.";

    private final String DATA_INBOUND_TOTAL_COUNTER_NAME = METRICS_PREFIX + "inbound.requests.total";
    private final String DATA_PARSED_ENTITIES_TOTAL_COUNTER_NAME = METRICS_PREFIX + "data.parsed.entities";
    private final String DATA_FILTERED_ENTITIES_TOTAL_COUNTER_NAME = METRICS_PREFIX + "data.filtered.entities";

    private final String GTFSRT_ENTITIES_TOTAL = METRICS_PREFIX + "gtfsrt.entitites.total";

    private final Map<String, Counter> parsedEntityCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> filteredEntityCounters = new ConcurrentHashMap<>();

    public PrometheusMetricsService() {
        super(PrometheusConfig.DEFAULT);
    }

    @PreDestroy
    public void shutdown() {
        this.close();
    }

    public void registerIncomingRequest(String dataType, long total) {
        List<Tag> counterTags = new ArrayList<>();
        counterTags.add(new ImmutableTag("dataType", dataType));

        counter(DATA_INBOUND_TOTAL_COUNTER_NAME, counterTags).increment(total);
    }

    public void registerIncomingEntity(String dataType, Boolean filtered) {
        if (filtered) {
            filteredEntityCounters.computeIfAbsent(dataType, type -> counter(DATA_FILTERED_ENTITIES_TOTAL_COUNTER_NAME, "dataType", type)).increment();
        } else {
            parsedEntityCounters.computeIfAbsent(dataType, type -> counter(DATA_PARSED_ENTITIES_TOTAL_COUNTER_NAME, "dataType", type)).increment();
        }
    }

    public synchronized void registerTotalGtfsRtEntities(int etCount, int vmCount, int sxCount) {
        // Create list copy to avoid concurrent modification
        List<Meter> metersToRemove = new ArrayList<>();
        for (Meter meter : this.getMeters()) {
            if (GTFSRT_ENTITIES_TOTAL.equals(meter.getId().getName())) {
                metersToRemove.add(meter);
            }
        }

        // Remove after iteration
        for (Meter meter : metersToRemove) {
            this.remove(meter);
        }

        List<Tag> counterTags = new ArrayList<>();
        counterTags.add(new ImmutableTag("dataType", "SIRI_ET"));
        super.gauge(GTFSRT_ENTITIES_TOTAL, counterTags, BigInteger.valueOf(etCount), BigInteger::doubleValue);

        counterTags = new ArrayList<>();
        counterTags.add(new ImmutableTag("dataType", "SIRI_VM"));
        super.gauge(GTFSRT_ENTITIES_TOTAL, counterTags, BigInteger.valueOf(vmCount), BigInteger::doubleValue);

        counterTags = new ArrayList<>();
        counterTags.add(new ImmutableTag("dataType", "SIRI_SX"));
        super.gauge(GTFSRT_ENTITIES_TOTAL, counterTags, BigInteger.valueOf(sxCount), BigInteger::doubleValue);
    }
}
