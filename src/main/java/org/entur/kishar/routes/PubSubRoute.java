package org.entur.kishar.routes;


import com.google.transit.realtime.GtfsRealtime;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.paho.PahoConstants;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.www.siri.SiriType;

import java.util.Map;
import java.util.UUID;

import static org.entur.kishar.routes.helpers.MqttHelper.buildTopic;


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
                    .bean("direct:send.vehicle.position.to.mqtt")
            ;

            from(siriSxTopic)
                    .wireTap("direct:log.incoming.siri.sx")
                    .to("direct:parse.siri.to.gtfs.rt.alerts")
                    .to("direct:register.gtfs.rt.alerts")
                    .process(p -> {
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
                        Map<byte[], GtfsRtData> body = siriToGtfsRealtimeService.convertSiriEtToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.trip.updates")
                    .process( p -> {
                        final Map<byte[], GtfsRtData> tripUpdate = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtTripUpdates(tripUpdate);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
                        Map<byte[], GtfsRtData> body = siriToGtfsRealtimeService.convertSiriVmToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final Map<byte[], GtfsRtData> vehiclePosition = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtVehiclePosition(vehiclePosition);
                        p.getOut().setBody(vehiclePosition.values());
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.alerts")
                    .process( p -> {
                        final byte[] data = (byte[]) p.getIn().getBody();
                        final SiriType siri = SiriType.parseFrom(data);
                        Map<byte[], GtfsRtData> body = siriToGtfsRealtimeService.convertSiriSxToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from ("direct:register.gtfs.rt.alerts")
                    .process( p -> {
                        final Map<byte[], GtfsRtData> alert = p.getIn().getBody(Map.class);
                        siriToGtfsRealtimeService.registerGtfsRtAlerts(alert);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from("direct:log.incoming.siri.et")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_ET", 1, false);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from("direct:log.incoming.siri.vm")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_VM", 1, false);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from("direct:log.incoming.siri.sx")
                    .process( p -> {
                        metrics.registerIncomingEntity("SIRI_SX", 1, false);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                    })
            ;

            from("direct:send.vehicle.position.to.mqtt")
                    .bean(metrics, "registerReceivedMqttMessage(\"SIRI_VM\")")
                    .choice().when(body().isNotNull())
                    .split(body())
                    .process(p -> {
                        final GtfsRtData body = p.getIn().getBody(GtfsRtData.class);
                        if (body.getData() != null) {
                            GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.parseFrom(body.getData());
                            if (feedEntity != null && feedEntity.hasVehicle()) {
                                GtfsRealtime.VehiclePosition vehiclePosition = feedEntity.getVehicle();
                                String topic = buildTopic(vehiclePosition);
                                if (topic != null) {
                                    p.getOut().setBody(vehiclePosition.toByteArray());
                                    p.getOut().setHeaders(p.getIn().getHeaders());
                                    p.getOut().setHeader(PahoConstants.CAMEL_PAHO_OVERRIDE_TOPIC, topic);
                                }
                            }
                        }
                    })
                    .choice().when(header(PahoConstants.CAMEL_PAHO_OVERRIDE_TOPIC).isNotNull())
                        .to("direct:send.to.mqtt")
                    .endChoice()
                    .end()
                    ;
        }
    }

}
