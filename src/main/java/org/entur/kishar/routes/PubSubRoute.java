package org.entur.kishar.routes;


import org.apache.camel.builder.RouteBuilder;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

@Service
@Configuration
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
                .setHeader("type", simple("SIRI_ET"))
                .wireTap("direct:log.incoming.data")
                .to("direct:parse.siri.to.gtfs.rt.trip.updates")
                .to("direct:register.gtfs.rt.trip.updates")
            ;

            from(siriVmTopic)
                .setHeader("type", simple("SIRI_VM"))
                .wireTap("direct:log.incoming.data")
                .to("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                .to("direct:register.gtfs.rt.vehicle.positions")
            ;

            from(siriSxTopic)
                .setHeader("type", simple("SIRI_SX"))
                .wireTap("direct:log.incoming.data")
                .to("direct:parse.siri.to.gtfs.rt.alerts")
                .to("direct:register.gtfs.rt.alerts")
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriEtToGtfsRt(
                                EstimatedVehicleJourneyRecord.fromByteBuffer(ByteBuffer.wrap(data))
                        );
                        p.getMessage().setBody(body);
                        p.getMessage().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.trip.updates")
                    .process( p -> {
                        final Map<String, GtfsRtData> tripUpdate = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtTripUpdates(tripUpdate);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriVmToGtfsRt(
                                VehicleActivityRecord.fromByteBuffer(ByteBuffer.wrap(data))
                        );
                        p.getMessage().setBody(body);
                        p.getMessage().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final Map<String, GtfsRtData> vehiclePosition = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtVehiclePosition(vehiclePosition);
                        p.getMessage().setBody(vehiclePosition.keySet());
                        p.getMessage().setHeaders(p.getIn().getHeaders());
                        p.getMessage().setHeader("map", vehiclePosition);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.alerts")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriSxToGtfsRt(
                                PtSituationElementRecord.fromByteBuffer(ByteBuffer.wrap(data))
                        );
                        p.getMessage().setBody(body);
                        p.getMessage().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.alerts")
                    .process( p -> {
                        final Map<String, GtfsRtData> alert = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtAlerts(alert);
                    })
            ;

            from("direct:log.incoming.data")
                .bean(metrics, "registerIncomingEntity(${header.type}, 1, false)")
            ;

        }
    }

}
