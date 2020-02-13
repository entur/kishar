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

import com.google.transit.realtime.GtfsRealtime;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.component.paho.PahoConstants;
import org.apache.camel.model.dataformat.JaxbDataFormat;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.Siri;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.entur.kishar.routes.helpers.MqttHelper.buildTopic;

@Service
@Configuration
public class SiriIncomingRoute extends RestRouteBuilder {

    private static final String PATH_HEADER = "path";
    public static final String DATASOURCE_HEADER_NAME = "datasource";
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Value("${kishar.anshar.polling.url.et}")
    private String ansharUrlEt;
    @Value("${kishar.anshar.polling.url.vm}")
    private String ansharUrlVm;
    @Value("${kishar.anshar.polling.url.sx}")
    private String ansharUrlSx;

    private Map<String, ZonedDateTime> inProgress = new HashMap<>();

    @Value("${kishar.anshar.polling.period:15s}")
    private String pollingPeriod;

    @Autowired
    private PrometheusMetricsService metrics;

    @Value("${kishar.mqtt.enabled:false}")
    private boolean mqttEnabled;

    private final Namespaces siriNamespace = new Namespaces("siri", "http://www.siri.org.uk/siri");

    public SiriIncomingRoute(@Autowired SiriToGtfsRealtimeService siriToGtfsRealtimeService) {
        this.siriToGtfsRealtimeService = siriToGtfsRealtimeService;
    }

    @Override
    public void configure() {

        JaxbDataFormat dataFormatType = new JaxbDataFormat();

//        onException(Exception.class)
//                .handled(true)
//                .log("Fetching data from ${header." + PATH_HEADER + "} failed.")
//                .process(p -> stop(p.getIn().getHeader(PATH_HEADER, String.class)));

        String path_ET = getPath(ansharUrlEt);
        from("timer://kishar.polling_et?fixedRate=true&period=" + pollingPeriod)
                .choice()
                .when(p -> !isInProgress(path_ET))
                    .setHeader(PATH_HEADER, constant(path_ET))
                    .to("direct:polling")
                .endChoice()
                .routeId("kishar.polling.et")
        ;

        String path_SX = getPath(ansharUrlSx);
        from("timer://kishar.polling_sx?fixedRate=true&period=" + pollingPeriod)
                .choice()
                .when(p -> !isInProgress(path_SX))
                    .setHeader(PATH_HEADER, constant(path_SX))
                    .to("direct:polling")
                .endChoice()
                .routeId("kishar.polling.sx")
        ;

        from("direct:polling")
                .process(p -> start(p.getIn().getHeader(PATH_HEADER, String.class)))
                .log("Fetching data from ${header."+PATH_HEADER+"}")
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
                .to("direct:forward.siri.vm.to.mqtt")
        ;


        rest("/internal")
                .post("siri-vm").to("direct:forward.siri.vm.to.mqtt")
        ;

        from("direct:forward.siri.vm.to.mqtt")
                .choice().when(p -> mqttEnabled)
                    .process(p ->p.getOut().setBody(p.getIn().getBody(String.class)))
                    .wireTap("direct:process.single.vm")
                    .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"))
                    .setBody(constant(null))
                .end()
        ;

        from("direct:process.single.vm")
                .to("xslt:xsl/split.xsl").split().tokenizeXML("Siri").streaming()
                .process( p -> {
                    final Siri siri = p.getIn().getBody(Siri.class);
                    p.getOut().setBody(siriToGtfsRealtimeService.convertSingleSiriVmToGtfsRt(siri));
                    p.getOut().setHeaders(p.getIn().getHeaders());
                    p.getOut().setHeader(DATASOURCE_HEADER_NAME, siriToGtfsRealtimeService.resolveDataSource(siri));
                })
                .bean(metrics, "registerReceivedMqttMessage(${header." + DATASOURCE_HEADER_NAME + "})")
                .choice().when(body().isNotNull())
                    .process(p -> {
                        final GtfsRealtime.VehiclePosition body = p.getIn().getBody(GtfsRealtime.VehiclePosition.class);
                        String topic = buildTopic(body);

                        if (topic != null) {
                            p.getOut().setBody(body);
                            p.getOut().setHeaders(p.getIn().getHeaders());
                            p.getOut().setHeader(PahoConstants.CAMEL_PAHO_OVERRIDE_TOPIC, topic);
                        }
                    })
                    .choice().when(header(PahoConstants.CAMEL_PAHO_OVERRIDE_TOPIC).isNotNull())
                        .to("direct:send.to.mqtt")
                    .endChoice()
                .end()
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
