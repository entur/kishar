package org.entur.kishar.gtfsrt;

import org.entur.avro.realtime.siri.converter.jaxb2avro.Jaxb2AvroConverter;
import org.entur.avro.realtime.siri.helper.JsonReader;
import org.entur.avro.realtime.siri.helper.JsonWriter;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.kishar.gtfsrt.helpers.SiriJsonReader;
import org.entur.siri21.util.SiriXml;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSiriJsonReader {

    @Test
    public void testReadVehicleActivityMatchesLibraryReader() throws Exception {
        String json = JsonWriter.write(createVehicleActivity("TST:Vehicle:1"));

        assertEquals(JsonReader.readVehicleActivity(json), SiriJsonReader.readVehicleActivity(json));
    }

    @Test
    public void testReadEstimatedVehicleJourneyMatchesLibraryReader() throws Exception {
        String json = JsonWriter.write(createEstimatedVehicleJourney("TST:ServiceJourney:1"));

        assertEquals(JsonReader.readEstimatedVehicleJourney(json), SiriJsonReader.readEstimatedVehicleJourney(json));
    }

    @Test
    public void testReadPtSituationElementMatchesLibraryReader() throws Exception {
        String json = JsonWriter.write(Helper.createPtSituationElement("TST"));

        assertEquals(JsonReader.readPtSituationElement(json), SiriJsonReader.readPtSituationElement(json));
    }

    @Test
    public void testConsecutiveReadsReturnEachMessage() throws Exception {
        String first = JsonWriter.write(createVehicleActivity("TST:Vehicle:1"));
        String second = JsonWriter.write(createVehicleActivity("TST:Vehicle:2"));

        assertEquals("TST:Vehicle:1", vehicleRef(SiriJsonReader.readVehicleActivity(first)));
        assertEquals("TST:Vehicle:2", vehicleRef(SiriJsonReader.readVehicleActivity(second)));
    }

    @Test
    public void testReadAfterMalformedMessageSucceeds() throws Exception {
        String valid = JsonWriter.write(createVehicleActivity("TST:Vehicle:1"));
        String truncated = valid.substring(0, valid.length() / 2);

        assertThrows(Exception.class, () -> SiriJsonReader.readVehicleActivity(truncated));
        assertEquals("TST:Vehicle:1", vehicleRef(SiriJsonReader.readVehicleActivity(valid)));
    }

    @Test
    public void testConcurrentReadsReturnTheirOwnMessage() throws Exception {
        int threads = 8;
        int readsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                String vehicleRef = "TST:Vehicle:" + t;
                String json = JsonWriter.write(createVehicleActivity(vehicleRef));
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < readsPerThread; i++) {
                        assertEquals(vehicleRef, vehicleRef(SiriJsonReader.readVehicleActivity(json)));
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static String vehicleRef(VehicleActivityRecord activity) {
        return activity.getMonitoredVehicleJourney().getVehicleRef().toString();
    }

    private static VehicleActivityRecord createVehicleActivity(String vehicleRef) throws Exception {
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
                "                    <DataSource>TST</DataSource>\n" +
                "                    <VehicleLocation>\n" +
                "                        <Longitude>10.75</Longitude>\n" +
                "                        <Latitude>59.91</Latitude>\n" +
                "                    </VehicleLocation>\n" +
                "                    <VehicleRef>" + vehicleRef + "</VehicleRef>\n" +
                "                </MonitoredVehicleJourney>\n" +
                "            </VehicleActivity>\n" +
                "        </VehicleMonitoringDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";
        return toSiriRecord(xml).getServiceDelivery().getVehicleMonitoringDeliveries().get(0)
                .getVehicleActivities().get(0);
    }

    private static EstimatedVehicleJourneyRecord createEstimatedVehicleJourney(String datedVehicleJourneyRef) throws Exception {
        String xml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>2026-10-26T14:17:36+01:00</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <EstimatedTimetableDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>2026-10-26T14:17:36+01:00</ResponseTimestamp>\n" +
                "            <EstimatedJourneyVersionFrame>\n" +
                "                <RecordedAtTime>2026-10-26T14:17:36+01:00</RecordedAtTime>\n" +
                "                <EstimatedVehicleJourney>\n" +
                "                    <LineRef>TST:Line:1</LineRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2026-10-26</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>" + datedVehicleJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <DataSource>TST</DataSource>\n" +
                "                    <EstimatedCalls>\n" +
                "                        <EstimatedCall>\n" +
                "                            <StopPointRef>TST:Quay:1</StopPointRef>\n" +
                "                            <Order>1</Order>\n" +
                "                            <AimedDepartureTime>2026-10-26T14:20:00+01:00</AimedDepartureTime>\n" +
                "                            <ExpectedDepartureTime>2026-10-26T14:21:30+01:00</ExpectedDepartureTime>\n" +
                "                        </EstimatedCall>\n" +
                "                    </EstimatedCalls>\n" +
                "                </EstimatedVehicleJourney>\n" +
                "            </EstimatedJourneyVersionFrame>\n" +
                "        </EstimatedTimetableDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";
        return toSiriRecord(xml).getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0).getEstimatedVehicleJourneys().get(0);
    }

    private static SiriRecord toSiriRecord(String xml) throws Exception {
        return Jaxb2AvroConverter.convert(SiriXml.parseXml(xml));
    }
}
