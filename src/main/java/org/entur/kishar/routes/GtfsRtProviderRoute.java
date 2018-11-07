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
    public void configure() throws Exception {

        rest("api")
                .get("/trip-updates").to("direct:kishar.get.tripUpdates")
                .get("/vehicle-positions").to("direct:kishar.get.vehiclePositions")
                .get("/alerts").to("direct:kishar.get.alerts")
        ;


        from("direct:kishar.get.tripUpdates")
                .bean(siriToGtfsRealtimeService, "getTripUpdates(${header.Content-Type})")
        ;
        from("direct:kishar.get.vehiclePositions")
                .bean(siriToGtfsRealtimeService, "getVehiclePositions(${header.Content-Type})")
        ;
        from("direct:kishar.get.alerts")
                .bean(siriToGtfsRealtimeService, "getAlerts(${header.Content-Type})")
        ;


        from("quartz2://kishar.update.output?fireNow=true&trigger.repeatInterval=10000")
                .log("Writing output")
                .bean(siriToGtfsRealtimeService, "writeOutput()")
                .routeId("kishar.update.output")
        ;
    }
}
