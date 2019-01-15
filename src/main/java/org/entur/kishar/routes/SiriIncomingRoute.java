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
package org.entur.kishar.routes;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.model.dataformat.JaxbDataFormat;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Configuration
public class SiriIncomingRoute extends RouteBuilder {

    private static final java.lang.String PATH_HEADER = "path";
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Value("${kishar.anshar.polling.url.et}")
    private String ansharUrlEt;
    @Value("${kishar.anshar.polling.url.vm}")
    private String ansharUrlVm;
    @Value("${kishar.anshar.polling.url.sx}")
    private String ansharUrlSx;

    private Map<String, ZonedDateTime> inProgress = new HashMap<>();

    @Value("${kishar.anshar.polling.interval.sec}")
    private int pollingIntervalSec;

    private final Namespaces siriNamespace = new Namespaces("siri", "http://www.siri.org.uk/siri");

    public SiriIncomingRoute(@Autowired SiriToGtfsRealtimeService siriToGtfsRealtimeService) {
        this.siriToGtfsRealtimeService = siriToGtfsRealtimeService;
    }

    @Override
    public void configure() {

        JaxbDataFormat dataFormatType = new JaxbDataFormat();

        String path_VM = getPath(ansharUrlVm);
        from("quartz2://kishar.polling_vm?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                .choice()
                .when(p -> !isInProgress(path_VM))
                    .setHeader(PATH_HEADER, constant(path_VM))
                    .to("direct:polling")
                .endChoice()
                .routeId("kishar.polling.vm")
        ;

        String path_ET = getPath(ansharUrlEt);
        from("quartz2://kishar.polling_et?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                .choice()
                .when(p -> !isInProgress(path_ET))
                    .setHeader(PATH_HEADER, constant(path_ET))
                    .to("direct:polling")
                .endChoice()
                .routeId("kishar.polling.et")
        ;

        String path_SX = getPath(ansharUrlSx);
        from("quartz2://kishar.polling_sx?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                .choice()
                .when(p -> !isInProgress(path_SX))
                    .setHeader(PATH_HEADER, constant(path_SX))
                    .to("direct:polling")
                .endChoice()
                .routeId("kishar.polling.sx")
        ;

        from("direct:polling")
                .process(p -> start(p.getIn().getHeader(PATH_HEADER, String.class)))
                .log("Fetching data from ${header." + PATH_HEADER +"}")
                .toD("${header." + PATH_HEADER +"}")
                .to("direct:process.helpers.xml")
                .choice()
                    .when().xpath("/siri:Siri/siri:ServiceDelivery/siri:MoreData='true'", siriNamespace)
                        .log("MoreData - fetching immediately from ${header." + PATH_HEADER +"}")
                        .to("direct:polling")
                    .endChoice()
                .end()
                .process(p -> stop(p.getIn().getHeader(PATH_HEADER, String.class)))
        ;

        from("direct:process.helpers.xml")
                .marshal(dataFormatType)
                .bean(siriToGtfsRealtimeService, "processDelivery(${body})")
        ;

    }

    private String getPath(String url) {
        if (url.contains("?")) {
            url += "&";
        } else {
            url += "?";
        }
        return url + "requestorId=kishar-" + UUID.randomUUID();
    }

    private void start(String path) {
        if (!inProgress.containsKey(path)) {
            inProgress.put(path, ZonedDateTime.now());
        }
    }
    private void stop(String path) {
        ZonedDateTime removed = inProgress.remove(path);
        if (removed != null) {
            log.info("Fetching all data from {} took {} sec", path, (ZonedDateTime.now().toEpochSecond()-removed.toEpochSecond()));
        }
    }

    private boolean isInProgress(String path) {
        return inProgress.containsKey(path);
    }
}
