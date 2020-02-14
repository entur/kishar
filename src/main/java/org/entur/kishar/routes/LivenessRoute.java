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

import org.apache.camel.Exchange;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LivenessRoute extends RestRouteBuilder {

    @Autowired
    PrometheusMetricsService prometheusRegistry;

    @Override
    public void configure() throws Exception {

        super.configure();

        rest("/health/")
                .get("ready").route().transform().constant("OK").routeId("kishar.rest.health.ready").endRest()
                .get("up").route().transform().constant("OK").routeId("kishar.rest.health.up").endRest()
                .get("healthy").route().transform().constant("OK").routeId("kishar.rest.health.healthy").endRest()
                .get("scrape").to("direct:scrape").id("kishar.rest.health.scrape")
        ;

        // Application is ready to accept traffic
        from("direct:scrape")
                .process(p -> {
                    if (prometheusRegistry != null) {
                        p.getOut().setBody(prometheusRegistry.scrape());
                    }
                })
                .setHeader(Exchange.CONTENT_TYPE, constant("text/plain"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"))
                .routeId("kishar.health.scrape")
        ;
    }
}
