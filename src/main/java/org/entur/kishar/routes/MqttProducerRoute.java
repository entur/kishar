package org.entur.kishar.routes;


import org.apache.camel.ThreadPoolRejectedPolicy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolProfileBuilder;
import org.apache.camel.component.paho.PahoConstants;
import org.apache.camel.spi.ThreadPoolProfile;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.entur.kishar.routes.SiriIncomingRoute.DATASOURCE_HEADER_NAME;

@Service
public class MqttProducerRoute extends RouteBuilder {
    private static Logger LOG = LoggerFactory.getLogger(MqttProducerRoute.class);

    private static final String clientId = UUID.randomUUID().toString();

    @Value("${kishar.mqtt.enabled:false}")
    private boolean mqttEnabled;

    @Value("${kishar.mqtt.host}")
    private String host;

    @Value("${kishar.mqtt.username}")
    private String username;

    @Value("${kishar.mqtt.password}")
    private String password;

    @Value("${kishar.mqtt.retain.data:true}")
    private boolean retain;

    private AtomicInteger counter = new AtomicInteger();

    @Autowired
    private PrometheusMetricsService metrics;

    @Bean
    MqttConnectOptions connectOptions() {
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setServerURIs(new String[] {host});
        connectOptions.setUserName(username);
        connectOptions.setPassword(password.toCharArray());
        connectOptions.setMaxInflight(1000);
        connectOptions.setAutomaticReconnect(true);
        return connectOptions;
    }

    @Override
    public void configure() {


        if (mqttEnabled) {

            LOG.info("Push to MQTT enabled: host: {}, username: {}, password: {}", host, username, password != null ? ""+password.length() + " chars":null);

            ThreadPoolProfile mqttThreadPool = new ThreadPoolProfileBuilder("mqtt-tp-profile")
                    .maxPoolSize(1000)
                    .maxQueueSize(2000)
                    .poolSize(50)
                    .rejectedPolicy(ThreadPoolRejectedPolicy.DiscardOldest)
                    .build();

            getContext().getExecutorServiceManager().registerThreadPoolProfile(mqttThreadPool);

            from("direct:send.to.mqtt")
                    .routeId("send.to.mqtt")
                    .setHeader(PahoConstants.CAMEL_PAHO_OVERRIDE_TOPIC, simple("${header.topic}"))
                    .wireTap("direct:post.to.paho.client").executorServiceRef("mqtt-tp-profile")
                    .setBody(simple(null))
            ;

            from("direct:post.to.paho.client")
                    .to("paho:default/topic?retained=" + retain + "&qos=1&clientId=" + clientId)
                    .bean(metrics, "registerSentMqttMessage(${header." + DATASOURCE_HEADER_NAME + "})")
                    .to("direct:log.mqtt.traffic");

            from("direct:log.mqtt.traffic")
                    .routeId("log.mqtt")
                    .process(p -> {
                        if (counter.incrementAndGet() % 10000 == 0) {
                            p.getOut().setHeader("counter", counter.get());
                        }
                    })
                    .choice()
                    .when(header("counter").isNotNull())
                        .log("MQTT: Published ${header.counter} updates")
                    .endChoice()
                    .end();

        } else {
            log.info("MQTT is disabled - will NOT post updates");

            from("direct:send.to.mqtt")
                    .process(p -> {
                        if (counter.incrementAndGet() % 1000 == 0) {
                            p.getOut().setHeader("counter", counter.get());
                        }
                    })
                    .bean(metrics, "registerSentMqttMessage(${header." + DATASOURCE_HEADER_NAME + "})")
                    .choice().when(header("counter").isNotNull())
                        .log("MQTT still disabled - attempted ${header.counter} updates")
                    .endChoice()
                    .end()
                    .setBody(simple(null))
            ;
        }
    }

}
