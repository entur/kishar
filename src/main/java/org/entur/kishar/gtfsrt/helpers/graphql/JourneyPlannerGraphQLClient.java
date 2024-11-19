package org.entur.kishar.gtfsrt.helpers.graphql;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.entur.kishar.gtfsrt.helpers.graphql.model.DatedServiceJourney;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

@Service
public class JourneyPlannerGraphQLClient {

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

    Data executeQuery(String query) {
        if (webClient == null) {
            return null;
        }

        Response graphqlResponse = webClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(query)
                .retrieve()
                .bodyToMono(Response.class)
                .block();

        return graphqlResponse == null ? null : graphqlResponse.data;
    }
}
/*
 * Internal wrapper-classes for GraphQL-response
 */

class Response {
    Data data;
    Response() {}
    public void setData(Data data) {
        this.data = data;
    }
}
class Data {
    DatedServiceJourney datedServiceJourney;
    Data() {}

    public void setDatedServiceJourney(DatedServiceJourney datedServiceJourney) {
        this.datedServiceJourney = datedServiceJourney;
    }
}
