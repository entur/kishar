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

import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Service
@Configuration
public class GtfsRtProviderRoute extends RestRouteBuilder {

    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    public GtfsRtProviderRoute(@Autowired SiriToGtfsRealtimeService siriToGtfsRealtimeService) {
        this.siriToGtfsRealtimeService = siriToGtfsRealtimeService;
    }

    @Override
    public void configure() {

        super.configure();

        rest("/api/")
                .get("trip-updates").to("direct:getTripUpdates").produces("application/octet-stream").id("kishar.trip-updates")
                .get("vehicle-positions").to("direct:getVehiclePositions").produces("application/octet-stream").id("kishar.vehicle-positions")
                .get("alerts").to("direct:getAlerts").produces("application/octet-stream").id("kishar.alerts")
                .get("debug/status").to("direct:getStatus").produces("application/text").id("kishar.status")
                .get("debug/reset").to("direct:reset").produces("application/text").id("kishar.status")
        ;


        from("direct:getStatus")
            .routeId("kishar.getStatus")
            .bean(siriToGtfsRealtimeService, "getStatus()")
            ;


        from("direct:reset")
            .routeId("kishar.reset")
            .bean(siriToGtfsRealtimeService, "reset()")
            ;

        from("direct:getTripUpdates")
                .routeId("kishar.getTripUpdates")
                .bean(siriToGtfsRealtimeService, "getTripUpdates(${header.Content-Type},${header.datasource})")
                .setHeader("Content-Disposition", constant("attachment; filename=trip-updates.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("direct:getVehiclePositions")
                .routeId("kishar.getVehiclePositions")
                .bean(siriToGtfsRealtimeService, "getVehiclePositions(${header.Content-Type},${header.datasource})")
                .setHeader("Content-Disposition", constant("attachment; filename=vehicle-positions.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("direct:getAlerts")
                .routeId("kishar.getAlerts")
                .bean(siriToGtfsRealtimeService, "getAlerts(${header.Content-Type},${header.datasource})")
                .setHeader("Content-Disposition", constant("attachment; filename=alerts.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("timer://kishar.update.output?fixedRate=true&period=10s")
                .bean(siriToGtfsRealtimeService, "writeOutput()")
                .routeId("kishar.update.output")
        ;
    }
}
