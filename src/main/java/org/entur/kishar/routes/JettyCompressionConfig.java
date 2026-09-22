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

import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the Jetty handler that {@link RestRouteBuilder} wires into its shared
 * restConfiguration() via the "handlers" endpoint property, so that GTFS-RT feed
 * responses are gzip-compressed whenever the client sends Accept-Encoding: gzip.
 */
@Configuration
public class JettyGzipConfig {

    @Bean(name = "gzipHandler")
    public GzipHandler gzipHandler() {
        GzipHandler gzipHandler = new GzipHandler();
        gzipHandler.setMinGzipSize(0);
        return gzipHandler;
    }
}
