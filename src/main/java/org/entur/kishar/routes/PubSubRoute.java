package org.entur.kishar.routes;


import org.apache.camel.builder.RouteBuilder;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.www.siri.SiriType;

import java.util.Map;
import java.util.UUID;


@Service
public class PubSubRoute extends RouteBuilder {
    private static Logger LOG = LoggerFactory.getLogger(PubSubRoute.class);

    private static final String clientId = UUID.randomUUID().toString();

    @Value("${kishar.pubsub.enabled:false}")
    private boolean pubsubEnabled;

    @Value("${kishar.pubsub.topic.et}")
    private String siriEtTopic;

    @Value("${kishar.pubsub.topic.vm}")
    private String siriVmTopic;

    @Value("${kishar.pubsub.topic.sx}")
    private String siriSxTopic;

    @Autowired
    private PrometheusMetricsService metrics;

    @Autowired
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Override
    public void configure() {


        if (pubsubEnabled) {

            from(siriEtTopic)
                    .wireTap("direct:log.incoming.siri.et")
                    .to("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .to("direct:register.gtfs.rt.trip.updates")
            ;

            from(siriVmTopic)
                    .wireTap("direct:log.incoming.siri.vm")
                    .to("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .to("direct:register.gtfs.rt.vehicle.positions")
            ;

            from(siriSxTopic)
                    .wireTap("direct:log.incoming.siri.sx")
                    .to("direct:parse.siri.to.gtfs.rt.alerts")
                    .to("direct:register.gtfs.rt.alerts")
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
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
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
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
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
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
