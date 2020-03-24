package org.entur.kishar.routes;


import com.google.transit.realtime.GtfsRealtime;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.Siri;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.camel.Exchange.LOOP_INDEX;
import static org.entur.kishar.routes.SiriIncomingRoute.DATASOURCE_HEADER_NAME;
import static org.entur.kishar.routes.SiriIncomingRoute.LIST_COUNT_HEADER_NAME;

@Service
public class PubSubRoute extends RouteBuilder {
    private static Logger LOG = LoggerFactory.getLogger(PubSubRoute.class);

    private static final String clientId = UUID.randomUUID().toString();

    @Value("${kishar.pubsub.enabled:false}")
    private boolean pubsubEnabled;

    @Value("${kishar.pubsub.topic.vm}")
    private String siriVmTopic;

    @Autowired
    private PrometheusMetricsService metrics;

    @Autowired
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Override
    public void configure() {


        if (pubsubEnabled) {

            from("direct:send.to.pubsub.topic.siri.et")
                    .wireTap("direct:log.incoming.siri.et")
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .to("direct:register.gtfs.rt.trip.updates")
            ;

            from("direct:send.to.pubsub.topic.siri.vm")
                    .wireTap("direct:log.incoming.siri.vm")
                    .to(siriVmTopic)
            ;

            from(siriVmTopic)
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .to("direct:register.gtfs.rt.vehicle.positions")
            ;


            from("direct:send.to.pubsub.topic.siri.sx")
                    .wireTap("direct:log.incoming.siri.sx")
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.alerts")
                    .to("direct:register.gtfs.rt.alerts")
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        Map<byte[], byte[]> body = siriToGtfsRealtimeService.convertSiriEtToGtfsRt(siri);
                        p.getOut().setBody(body);
                    })
            ;

            from ("direct:register.gtfs.rt.trip.updates")
                    .process( p -> {
                        final Map<byte[], byte[]> tripUpdate = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtTripUpdates(tripUpdate);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        Map<byte[], byte[]> body = siriToGtfsRealtimeService.convertSiriVmToGtfsRt(siri);
                        p.getOut().setBody(body);
                    })
            ;

            from ("direct:register.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final Map<byte[], byte[]> vehiclePosition = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtVehiclePosition(vehiclePosition);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.alerts")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        Map<byte[], byte[]> body = siriToGtfsRealtimeService.convertSiriSxToGtfsRt(siri);
                        p.getOut().setBody(body);
                    })
            ;

            from ("direct:register.gtfs.rt.alerts")
                    .process( p -> {
                        final Map<byte[], byte[]> alert = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtAlerts(alert);
                    })
            ;

            from("direct:log.incoming.siri.et")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_ET", 1, false);
                    })
            ;

            from("direct:log.incoming.siri.vm")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_VM", 1, false);
                    })
            ;

            from("direct:log.incoming.siri.sx")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_SX", 1, false);
                    })
            ;
        }
    }

}
