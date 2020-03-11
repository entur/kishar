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

    @Value("${kishar.pubsub.topic.et}")
    private String siriEtTopic;

    @Value("${kishar.pubsub.topic.tripupdate}")
    private String gtfsRtTripUpdateTopic;

    @Value("${kishar.pubsub.topic.vm}")
    private String siriVmTopic;

    @Value("${kishar.pubsub.topic.vehicleposition}")
    private String gtfsRtVehiclePositionTopic;

    @Value("${kishar.pubsub.topic.sx}")
    private String siriSxTopic;

    @Value("${kishar.pubsub.topic.alert}")
    private String gtfsRtAlertTopic;

    @Autowired
    private PrometheusMetricsService metrics;

    @Autowired
    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Override
    public void configure() {


        if (pubsubEnabled) {

            from("direct:send.to.pubsub.topic.siri.et")
                    .wireTap("direct:log.incoming.siri.et")
                    .to(siriEtTopic)
            ;

            from(siriEtTopic + "?subId")
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .loop(header(LIST_COUNT_HEADER_NAME)).copy()
                        .process(exchange -> {
                            final List<GtfsRealtime.TripUpdate.Builder> tripUpdates = exchange.getIn().getBody(List.class);
                            exchange.getOut().setBody(tripUpdates.get(Integer.parseInt(property(LOOP_INDEX).toString())));
                            exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                        })
                        .to(gtfsRtTripUpdateTopic)
                    .end()
            ;

            from(gtfsRtTripUpdateTopic)
                    .to("direct:register.gtfs.rt.trip.updates")
            ;

            from("direct:send.to.pubsub.topic.siri.vm")
                    .wireTap("direct:log.incoming.siri.vm")
                    .to(siriVmTopic)
            ;

            from(siriVmTopic)
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .loop(header(LIST_COUNT_HEADER_NAME)).copy()
                    .process(exchange -> {
                        final List<GtfsRealtime.VehiclePosition.Builder> vehiclePositions = exchange.getIn().getBody(List.class);
                        exchange.getOut().setBody(vehiclePositions.get(Integer.parseInt(property(LOOP_INDEX).toString())));
                        exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                    })
                    .to(gtfsRtVehiclePositionTopic)
                    .end()
            ;

            from(gtfsRtVehiclePositionTopic)
                    .to("direct:register.gtfs.rt.vehicle.positions")
            ;

            from("direct:send.to.pubsub.topic.siri.sx")
                    .wireTap("direct:log.incoming.siri.sx")
                    .to(siriSxTopic)
            ;

            from(siriSxTopic)
                    .split().tokenizeXML("Siri").streaming()
                    .to("direct:parse.siri.to.gtfs.rt.alerts")
                    .loop(header(LIST_COUNT_HEADER_NAME)).copy()
                    .process(exchange -> {
                        final List<GtfsRealtime.Alert.Builder> alerts = exchange.getIn().getBody(List.class);
                        exchange.getOut().setBody(alerts.get(Integer.parseInt(property(LOOP_INDEX).toString())));
                        exchange.getOut().setHeaders(exchange.getIn().getHeaders());
                    })
                    .to(gtfsRtAlertTopic)
                    .end()
            ;

            from(gtfsRtAlertTopic)
                    .to("direct:register.gtfs.rt.alerts")
            ;

            from ("direct:parse.siri.to.gtfs.rt.trip.updates")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        List<GtfsRealtime.TripUpdate.Builder> body = siriToGtfsRealtimeService.convertSiriEtToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                        p.getOut().setHeader(LIST_COUNT_HEADER_NAME, body.size());
                        p.getOut().setHeader(DATASOURCE_HEADER_NAME, siriToGtfsRealtimeService.getDatasourceFromSiriEt(siri));
                    })
            ;

            from ("direct:register.gtfs.rt.trip.updates")
                    .process( p -> {
                        final GtfsRealtime.TripUpdate.Builder tripUpdate = p.getIn().getBody(GtfsRealtime.TripUpdate.Builder.class);
                        final String datasource = p.getIn().getHeader(DATASOURCE_HEADER_NAME, String.class);
                        siriToGtfsRealtimeService.registerGtfsRtTripUpdate(tripUpdate, datasource);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        List<GtfsRealtime.VehiclePosition.Builder> body = siriToGtfsRealtimeService.convertSiriVmToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                        p.getOut().setHeader(LIST_COUNT_HEADER_NAME, body.size());
                        p.getOut().setHeader(DATASOURCE_HEADER_NAME, siriToGtfsRealtimeService.getDatasourceFromSiriVm(siri));
                    })
            ;

            from ("direct:register.gtfs.rt.vehicle.positions")
                    .process( p -> {
                        final GtfsRealtime.VehiclePosition.Builder vehiclePosition = p.getIn().getBody(GtfsRealtime.VehiclePosition.Builder.class);
                        final String datasource = p.getIn().getHeader(DATASOURCE_HEADER_NAME, String.class);
                        siriToGtfsRealtimeService.registerGtfsRtVehiclePosition(vehiclePosition, datasource);
                    })
            ;

            from ("direct:parse.siri.to.gtfs.rt.alerts")
                    .process( p -> {
                        final Siri siri = p.getIn().getBody(Siri.class);
                        List<GtfsRealtime.Alert.Builder> body = siriToGtfsRealtimeService.convertSiriSxToGtfsRt(siri);
                        p.getOut().setBody(body);
                        p.getOut().setHeaders(p.getIn().getHeaders());
                        p.getOut().setHeader(LIST_COUNT_HEADER_NAME, body.size());
                        p.getOut().setHeader(DATASOURCE_HEADER_NAME, siriToGtfsRealtimeService.getDatasourceFromSiriSx(siri));
                    })
            ;

            from ("direct:register.gtfs.rt.alerts")
                    .process( p -> {
                        final GtfsRealtime.Alert.Builder alert = p.getIn().getBody(GtfsRealtime.Alert.Builder.class);
                        final String datasource = p.getIn().getHeader(DATASOURCE_HEADER_NAME, String.class);
                        siriToGtfsRealtimeService.registerGtfsRtAlert(alert, datasource);
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
