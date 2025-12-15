package org.entur.kishar.gtfsrt.helpers.graphql;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import jakarta.annotation.PreDestroy;
import org.entur.kishar.gtfsrt.helpers.graphql.model.DatedServiceJourney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

@Service
public class JourneyPlannerGraphQLClient {

    private static final Logger LOG = LoggerFactory.getLogger(JourneyPlannerGraphQLClient.class);
    private static final int HTTP_TIMEOUT_MILLISECONDS = 10000;

    private final WebClient webClient;

    public JourneyPlannerGraphQLClient(@Value("${kishar.journeyplanner.url:}") String graphQlUrl, @Value("${kishar.journeyplanner.EtClientName:}") String etClientNameHeader) {
        if (graphQlUrl == null || graphQlUrl.isEmpty()) {
            webClient = null;
        }else {
            webClient = WebClient.builder()
                    .baseUrl(graphQlUrl)
                    .defaultHeader("ET-Client-Name", etClientNameHeader)
                    .clientConnector(new ReactorClientHttpConnector(HttpClient.create().option(ChannelOption.CONNECT_TIMEOUT_MILLIS, HTTP_TIMEOUT_MILLISECONDS).doOnConnected(connection -> {
                        connection.addHandlerLast(new ReadTimeoutHandler(HTTP_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS));
                        connection.addHandlerLast(new WriteTimeoutHandler(HTTP_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS));
                    })))
                    .codecs(codecs -> codecs
                            .defaultCodecs()
                            .maxInMemorySize(500 * 1024))
                    .build();
        }
    }

    Mono<Data> executeQuery(String query) {
        if (webClient == null) {
            return Mono.empty();
        }

        return webClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(query)
                .retrieve()
                .bodyToMono(Response.class)
                .map(response -> response == null ? null : response.data)
                .onErrorResume(e -> Mono.empty()); // Handle errors gracefully
    }

    @PreDestroy
    public void shutdown() {
        if (webClient != null) {
            // WebClient itself doesn't need closing, but ensure connection pool is released
            // This is mostly handled by Spring, but we document it explicitly
            LOG.info("Shutting down GraphQL client");
        }
    }

    /*
     * Internal wrapper-classes for GraphQL-response
     */
    static class Response {
        Data data;
        Response() {}
        public void setData(Data data) {
            this.data = data;
        }
    }

    // Static inner class to allow access from ServiceJourneyService
    public static class Data {
        public DatedServiceJourney datedServiceJourney;
        Data() {}

        public void setDatedServiceJourney(DatedServiceJourney datedServiceJourney) {
            this.datedServiceJourney = datedServiceJourney;
        }
    }
}
