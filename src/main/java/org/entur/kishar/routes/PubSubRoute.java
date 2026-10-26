package org.entur.kishar.routes;


import org.apache.camel.builder.RouteBuilder;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriJsonReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PubSubRoute extends RouteBuilder {

    @Value("${kishar.pubsub.enabled:false}")
    private boolean pubsubEnabled;

    @Value("${kishar.pubsub.topic.et}")
    private String siriEtTopic;

    @Value("${kishar.pubsub.topic.vm}")
    private String siriVmTopic;

    @Value("${kishar.pubsub.topic.sx}")
    private String siriSxTopic;

    @Autowired
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Override
    public void configure() {


        if (pubsubEnabled) {

            from(siriEtTopic)
                .to("direct:parse.siri.to.gtfs.rt.trip.updates")
                .to("direct:register.gtfs.rt.trip.updates")
            ;

            from(siriVmTopic)
                .to("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                .to("direct:register.gtfs.rt.vehicle.positions")
            ;

            from(siriSxTopic)
                .to("direct:parse.siri.to.gtfs.rt.alerts")
                .to("direct:register.gtfs.rt.alerts")
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriEtToGtfsRt(
                                SiriJsonReader.readEstimatedVehicleJourney(p.getIn().getBody(String.class))
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
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriVmToGtfsRt(
                                SiriJsonReader.readVehicleActivity(p.getIn().getBody(String.class))
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
                        Map<String, GtfsRtData> body = siriToGtfsRealtimeService.convertSiriSxToGtfsRt(
                                SiriJsonReader.readPtSituationElement(p.getIn().getBody(String.class))
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

        }
    }

}
