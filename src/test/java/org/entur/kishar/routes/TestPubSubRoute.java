package org.entur.kishar.routes;

import org.apache.camel.ProducerTemplate;
import org.entur.avro.realtime.siri.converter.jaxb2avro.Jaxb2AvroConverter;
import org.entur.avro.realtime.siri.helper.JsonWriter;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.kishar.App;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.entur.siri21.util.SiriXml;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = App.class)
@TestPropertySource(properties = {
        // Separate port, since this context runs alongside the cached context of the other tests
        "kishar.incoming.port=0",
        "kishar.pubsub.enabled=true",
        "kishar.pubsub.topic.et=direct:test.et",
        "kishar.pubsub.topic.vm=direct:test.vm",
        "kishar.pubsub.topic.sx=direct:test.sx",
})
public class TestPubSubRoute {

    private static final String PARSED = "app.kishar.data.parsed.entities";
    private static final String FILTERED = "app.kishar.data.filtered.entities";

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private PrometheusMetricsService metrics;

    @Test
    public void testWhitelistedMessageIsCountedOnceAsParsed() throws Exception {
        double parsedBefore = count(PARSED, "SIRI_VM");
        double filteredBefore = count(FILTERED, "SIRI_VM");

        producerTemplate.sendBody("direct:test.vm", JsonWriter.write(createVehicleActivity("TST")));
        waitForAsyncProcessing();

        assertEquals(1, count(PARSED, "SIRI_VM") - parsedBefore);
        assertEquals(0, count(FILTERED, "SIRI_VM") - filteredBefore);
    }

    @Test
    public void testNonWhitelistedMessageIsCountedOnceAsFiltered() throws Exception {
        double parsedBefore = count(PARSED, "SIRI_VM");
        double filteredBefore = count(FILTERED, "SIRI_VM");

        producerTemplate.sendBody("direct:test.vm", JsonWriter.write(createVehicleActivity("NOT_WHITELISTED")));
        waitForAsyncProcessing();

        assertEquals(0, count(PARSED, "SIRI_VM") - parsedBefore);
        assertEquals(1, count(FILTERED, "SIRI_VM") - filteredBefore);
    }

    // Give any asynchronous counting (e.g. a wireTap) time to happen, so that double counting is caught
    private static void waitForAsyncProcessing() throws InterruptedException {
        Thread.sleep(500);
    }

    private double count(String name, String dataType) {
        var counter = metrics.find(name).tag("dataType", dataType).counter();
        return counter == null ? 0 : counter.count();
    }

    private static VehicleActivityRecord createVehicleActivity(String datasource) throws Exception {
        String xml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>2026-10-26T14:17:36+01:00</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <VehicleMonitoringDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>2026-10-26T14:17:36+01:00</ResponseTimestamp>\n" +
                "            <VehicleActivity>\n" +
                "                <RecordedAtTime>2026-10-26T14:17:36+01:00</RecordedAtTime>\n" +
                "                <ValidUntilTime>2026-10-26T14:27:36+01:00</ValidUntilTime>\n" +
                "                <MonitoredVehicleJourney>\n" +
                "                    <LineRef>TST:Line:1</LineRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2026-10-26</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>TST:ServiceJourney:1</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <DataSource>" + datasource + "</DataSource>\n" +
                "                    <VehicleLocation>\n" +
                "                        <Longitude>10.75</Longitude>\n" +
                "                        <Latitude>59.91</Latitude>\n" +
                "                    </VehicleLocation>\n" +
                "                    <VehicleRef>TST:Vehicle:1</VehicleRef>\n" +
                "                </MonitoredVehicleJourney>\n" +
                "            </VehicleActivity>\n" +
                "        </VehicleMonitoringDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";
        return Jaxb2AvroConverter.convert(SiriXml.parseXml(xml)).getServiceDelivery()
                .getVehicleMonitoringDeliveries().get(0).getVehicleActivities().get(0);
    }
}
