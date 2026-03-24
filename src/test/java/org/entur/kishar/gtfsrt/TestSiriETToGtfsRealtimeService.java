package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSiriETToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testAsyncGtfsRtProduction() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, stopCount, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);

        // GTFS-RT is produced asynchronously - should be empty at first
        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);


        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

    }

    @Test
    public void testAsyncGtfsRtProductionWithoutEstimates() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, stopCount, 30, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);

        // GTFS-RT is produced asynchronously - should be empty at first

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);


        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.TripUpdate tripUpdate = entityList.get(0).getTripUpdate();
        assertNotNull(tripUpdate);

        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdateList = tripUpdate.getStopTimeUpdateList();
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : stopTimeUpdateList) {
            if (stopTimeUpdate.hasArrival()) {
                assertFalse(stopTimeUpdate.getArrival().getDelay() < 0);
            }
            if (stopTimeUpdate.hasDeparture()) {
                assertFalse(stopTimeUpdate.getDeparture().getDelay() < 0);
            }
        }

        byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

    }

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriRecord siri) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriToGtfsRt(siri);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    @Test
    public void testEtToTripUpdate() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, stopCount, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.TripUpdate tripUpdate = entity.getTripUpdate();
        assertNotNull(tripUpdate);

        assertEquals(stopCount, tripUpdate.getStopTimeUpdateCount());

        assertNotNull(tripUpdate.getTrip());
        assertEquals(datedVehicleJourneyRef, tripUpdate.getTrip().getTripId());
        assertEquals(lineRefValue, tripUpdate.getTrip().getRouteId());

        for (int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
            GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate = tripUpdate.getStopTimeUpdate(i);

            int expectedStopSequence = i + 1; //First stop should have sequence 1
            assertEquals(expectedStopSequence, stopTimeUpdate.getStopSequence());

            //Assert arrival for all but first stop
            GtfsRealtime.TripUpdate.StopTimeEvent arrival = stopTimeUpdate.getArrival();
            assertNotNull(arrival);
            if (i == 0) {
                assertTrue(arrival.getAllFields().isEmpty());
            } else {
                assertFalse(arrival.getAllFields().isEmpty());
                assertEquals(delayPerStop, arrival.getDelay());
            }

            //Assert departure for all but last stop
            GtfsRealtime.TripUpdate.StopTimeEvent departure = stopTimeUpdate.getDeparture();
            assertNotNull(departure);
            if (i == stopCount-1) {
                assertTrue(departure.getAllFields().isEmpty());
            } else {
                assertFalse(departure.getAllFields().isEmpty());
                assertEquals(delayPerStop, departure.getDelay());
            }
        }
    }

    @Test
    public void testEtToTripUpdateFilterOnDatasource() {
        // Specifying local service for specific datasource-testing
        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef1 = "TST:ServiceJourney:1234";
        String datedVehicleJourneyRef2 = "TST:ServiceJourney:1235";
        String datasource1 = "TST";
        String datasource2 = "BNR";

        SiriRecord siriTST = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef1, datasource1);
        SiriRecord siriBNR = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef2, datasource2);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siriTST), RedisService.Type.TRIP_UPDATE);
        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siriBNR), RedisService.Type.TRIP_UPDATE);

        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", "TST");
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.TripUpdate tripUpdate = entity.getTripUpdate();
        assertNotNull(tripUpdate);

        assertEquals(1, tripUpdate.getStopTimeUpdateCount());

        tripUpdates = rtService.getTripUpdates("application/json", "BNR");
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        tripUpdate = entity.getTripUpdate();
        assertNotNull(tripUpdate);

        assertEquals(1, tripUpdate.getStopTimeUpdateCount());

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        assertEquals(2, entityList.size());
    }

    @Test
    public void testEtToTripUpdateNoWhitelist() {

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.TripUpdate tripUpdate = entity.getTripUpdate();
        assertNotNull(tripUpdate);

        assertEquals(1, tripUpdate.getStopTimeUpdateCount());

    }

    @Test
    public void testEtToTripUpdateIgnoreDatasourceNotInWhitelist() {

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "NSB";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());
    }

    @Test
    public void testEtWithoutFramedVehicleRef() {
        String lineRefValue = "TST:Line:1234";
        String datedVehicleJourneyRef = null;
        String datasource = "TST";

        // Assert that ET is ignored when framedVehicleRef is null
        SiriRecord siri = createSiriEtDelivery(lineRefValue, 5, 30, datedVehicleJourneyRef, datasource);
        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0)
                .getFramedVehicleJourneyRef());

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());


        datedVehicleJourneyRef = "TTT:ServiceJourney:1234";
        siri = createSiriEtDelivery(lineRefValue, 5, 30, datedVehicleJourneyRef, datasource);

        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0)
                .getFramedVehicleJourneyRef());

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

    }

    @Test
    public void testMappingOfSiriEt() {
        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord et = createSiriEtDelivery(lineRefValue, 5, delayPerStop, datedVehicleJourneyRef, datasource);

        Map<String, GtfsRtData> result = rtService.convertSiriToGtfsRt(et);

        assertFalse(result.isEmpty());
    }

    private SiriRecord createSiriEtDelivery(String lineRefValue, int calls, int delayPerStop, String datedVehicleJourneyRef, String datasource) {
        String startTime = ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String etXmlHead = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>" + startTime +"</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <EstimatedTimetableDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>" + startTime +"</ResponseTimestamp>\n" +
                "            <EstimatedJourneyVersionFrame>\n" +
                "                <RecordedAtTime>" + startTime +"</RecordedAtTime>\n" +
                "                <EstimatedVehicleJourney>\n" +
                "                    <RecordedAtTime>" + startTime +"</RecordedAtTime>\n" +
                "                    <LineRef>" + lineRefValue + "</LineRef>\n" +
                "                    <DirectionRef>0</DirectionRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2024-12-20</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>" + datedVehicleJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <VehicleMode>bus</VehicleMode>\n" +
                "                    <OriginName>Teste Hageby</OriginName>\n" +
                "                    <OperatorRef>" + datasource +":Operator:123</OperatorRef>\n" +
                "                    <Monitored>true</Monitored>\n" +
                "                    <DataSource>"+ datasource +"</DataSource>\n" +
                "                    <EstimatedCalls>\n";

        String etXmlCalls = createEstimatedCalls(calls, delayPerStop);

        String etXmlTail =
                "                    </EstimatedCalls>\n" +
                "                    <IsCompleteStopSequence>true</IsCompleteStopSequence>\n" +
                "                </EstimatedVehicleJourney>\n" +
                "            </EstimatedJourneyVersionFrame>\n" +
                "        </EstimatedTimetableDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";

        return createSiriRecord(etXmlHead + etXmlCalls + etXmlTail);
    }


    private String createEstimatedCalls(int stopCount, Integer addedDelayPerStop) {

        ZonedDateTime startTime = ZonedDateTime.now();
        StringBuilder callsXml = new StringBuilder();

        for (int i = 0; i < stopCount; i++) {
            String stopPointRef = "TST:Quay:1234-" + i;

            callsXml.append(
                    createCall(
                            stopPointRef,
                            i+1,
                    startTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                    startTime.plusSeconds(addedDelayPerStop).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                            i == 0,
                            i == stopCount-1
                            )
            );

            startTime = startTime.plusSeconds(addedDelayPerStop).plusSeconds(60);

        }
        return callsXml.toString();
    }

    private String createCall(String stopPointRef, int order, String aimedTime,
                              String expectedTime, boolean skipArrival, boolean skipDeparture) {
        return
                "                        <EstimatedCall>\n" +
                "                            <StopPointRef>" + stopPointRef + "</StopPointRef>\n" +
                "                            <Order>" + order +"</Order>\n" +
                        (skipArrival ? "":
                "                            <AimedArrivalTime>" + aimedTime + "</AimedArrivalTime>\n" +
                "                            <ExpectedArrivalTime>" + expectedTime + "</ExpectedArrivalTime>\n" +
                "                            <ArrivalStatus>delayed</ArrivalStatus>\n" +
                "                            <ArrivalBoardingActivity>noAlighting</ArrivalBoardingActivity>\n"
                        ) +
                        (skipDeparture ? "":
                "                            <AimedDepartureTime>" + aimedTime + "</AimedDepartureTime>\n" +
                "                            <ExpectedDepartureTime>"+ expectedTime + "</ExpectedDepartureTime>\n" +
                "                            <DepartureStatus>delayed</DepartureStatus>\n"
                        ) +
                "                        </EstimatedCall>\n";
    }

    /**
     * Bug 1 regression test: an intermediate EstimatedCall with a missing StopPointRef should be
     * skipped (continue), not cause the remaining stops to be dropped (return).
     * With the bug present the TripUpdate would contain only the first stop; after the fix it
     * must contain two stop-time updates (stops 1 and 3, stop 2 skipped).
     */
    @Test
    public void testNullStopPointRefInMiddleCallDoesNotTruncateStopList() throws Exception {
        String lineRefValue = "TST:Line:1234";
        String datedVehicleJourneyRef = "TST:ServiceJourney:9999";
        String datasource = "TST";

        String startTime = ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String aimedTime = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String expectedTime = ZonedDateTime.now().plusSeconds(30).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        // Call 1 - normal stop
        String call1 = "                        <EstimatedCall>\n" +
                "                            <StopPointRef>TST:Quay:stop-1</StopPointRef>\n" +
                "                            <Order>1</Order>\n" +
                "                            <AimedDepartureTime>" + aimedTime + "</AimedDepartureTime>\n" +
                "                            <ExpectedDepartureTime>" + expectedTime + "</ExpectedDepartureTime>\n" +
                "                            <DepartureStatus>delayed</DepartureStatus>\n" +
                "                        </EstimatedCall>\n";

        // Call 2 - StopPointRef intentionally omitted to simulate null stopPointRef
        String call2 = "                        <EstimatedCall>\n" +
                "                            <Order>2</Order>\n" +
                "                            <AimedArrivalTime>" + aimedTime + "</AimedArrivalTime>\n" +
                "                            <ExpectedArrivalTime>" + expectedTime + "</ExpectedArrivalTime>\n" +
                "                            <ArrivalStatus>delayed</ArrivalStatus>\n" +
                "                            <AimedDepartureTime>" + aimedTime + "</AimedDepartureTime>\n" +
                "                            <ExpectedDepartureTime>" + expectedTime + "</ExpectedDepartureTime>\n" +
                "                            <DepartureStatus>delayed</DepartureStatus>\n" +
                "                        </EstimatedCall>\n";

        // Call 3 - normal stop
        String call3 = "                        <EstimatedCall>\n" +
                "                            <StopPointRef>TST:Quay:stop-3</StopPointRef>\n" +
                "                            <Order>3</Order>\n" +
                "                            <AimedArrivalTime>" + aimedTime + "</AimedArrivalTime>\n" +
                "                            <ExpectedArrivalTime>" + expectedTime + "</ExpectedArrivalTime>\n" +
                "                            <ArrivalStatus>delayed</ArrivalStatus>\n" +
                "                        </EstimatedCall>\n";

        String xml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>" + startTime + "</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <EstimatedTimetableDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>" + startTime + "</ResponseTimestamp>\n" +
                "            <EstimatedJourneyVersionFrame>\n" +
                "                <RecordedAtTime>" + startTime + "</RecordedAtTime>\n" +
                "                <EstimatedVehicleJourney>\n" +
                "                    <RecordedAtTime>" + startTime + "</RecordedAtTime>\n" +
                "                    <LineRef>" + lineRefValue + "</LineRef>\n" +
                "                    <DirectionRef>0</DirectionRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2024-12-20</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>" + datedVehicleJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <VehicleMode>bus</VehicleMode>\n" +
                "                    <OperatorRef>" + datasource + ":Operator:123</OperatorRef>\n" +
                "                    <Monitored>true</Monitored>\n" +
                "                    <DataSource>" + datasource + "</DataSource>\n" +
                "                    <EstimatedCalls>\n" +
                call1 + call2 + call3 +
                "                    </EstimatedCalls>\n" +
                "                    <IsCompleteStopSequence>true</IsCompleteStopSequence>\n" +
                "                </EstimatedVehicleJourney>\n" +
                "            </EstimatedJourneyVersionFrame>\n" +
                "        </EstimatedTimetableDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";

        SiriRecord siri = createSiriRecord(xml);
        assertNotNull(siri);

        EstimatedVehicleJourneyRecord evj = siri.getServiceDelivery()
                .getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0);

        Map<String, GtfsRtData> result = rtService.convertSiriEtToGtfsRt(evj);
        assertFalse(result.isEmpty());

        GtfsRtData gtfsRtData = result.values().iterator().next();
        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.parseFrom(gtfsRtData.getData());
        GtfsRealtime.TripUpdate tripUpdate = feedEntity.getTripUpdate();

        // Stop 2 (null stopPointRef) must be skipped; stops 1 and 3 must be present
        assertEquals(2, tripUpdate.getStopTimeUpdateCount(),
                "Expected 2 stop-time updates (stop 1 and stop 3); stop 2 with null stopPointRef must be skipped, not truncate the list");
    }

    /**
     * Bug 2 regression test (positive path): when a FramedVehicleJourneyRef is present the
     * TripDescriptor's getTripId() returns a non-empty string, not null. The guard must use
     * isEmpty() rather than a null check so that the TripDescriptor is actually set on the
     * TripUpdate.
     */
    @Test
    public void testTripDescriptorIsSetWhenFramedVehicleJourneyRefPresent() throws Exception {
        String lineRefValue = "TST:Line:1234";
        String datedVehicleJourneyRef = "TST:ServiceJourney:5678";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, 1, 30, datedVehicleJourneyRef, datasource);
        assertNotNull(siri);

        EstimatedVehicleJourneyRecord evj = siri.getServiceDelivery()
                .getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0);

        Map<String, GtfsRtData> result = rtService.convertSiriEtToGtfsRt(evj);
        assertFalse(result.isEmpty());

        GtfsRtData gtfsRtData = result.values().iterator().next();
        GtfsRealtime.FeedEntity feedEntity = GtfsRealtime.FeedEntity.parseFrom(gtfsRtData.getData());
        GtfsRealtime.TripUpdate tripUpdate = feedEntity.getTripUpdate();

        assertTrue(tripUpdate.hasTrip(), "TripDescriptor must be set when FramedVehicleJourneyRef is present");
        assertFalse(tripUpdate.getTrip().getTripId().isEmpty(),
                "TripId must not be empty when FramedVehicleJourneyRef is present");
        assertFalse(tripUpdate.getTrip().getStartDate().isEmpty(),
                "StartDate must not be empty when FramedVehicleJourneyRef is present");

        assertEquals(datedVehicleJourneyRef, tripUpdate.getTrip().getTripId());
    }

    /**
     * Bug 2 regression test (negative / guard path): when a DatedVehicleJourneyRef is present but
     * cannot be resolved (no FramedVehicleJourneyRef and the ServiceJourneyService lookup returns
     * a ServiceJourney with a null id), getEstimatedVehicleJourneyAsTripDescriptor never calls
     * setTripId(), so getTripId() returns "" (empty string, never null in protobuf).
     *
     * <p>With the old {@code != null} guard, setTrip() would have been called with an empty
     * TripDescriptor (wrong). The fix uses {@code !isEmpty()}, so the trip must NOT be set.
     *
     * <p>A DatedVehicleJourneyRef containing ":DatedServiceJourney:" triggers a GraphQL lookup in
     * ServiceJourneyService. In the test environment no real GraphQL server is available, so the
     * lookup fails and returns a ServiceJourney with id == null — causing setTripId() to be skipped.
     * The EVJ passes checkPreconditions() because it has a non-null DatedVehicleJourneyRef.
     */
    @Test
    public void testTripDescriptorNotSetWhenTripIdWouldBeEmpty() throws Exception {
        String lineRefValue = "TST:Line:1234";
        String datasource = "TST";
        // Use ":DatedServiceJourney:" so ServiceJourneyService attempts a GraphQL lookup,
        // which will fail in the test environment and return a ServiceJourney with id == null.
        String datedServiceJourneyRef = "TST:DatedServiceJourney:unresolvable-9999";
        String startTime = ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String aimedTime = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String expectedTime = ZonedDateTime.now().plusSeconds(30).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        // Build XML without FramedVehicleJourneyRef — only DatedVehicleJourneyRef is set
        String xml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>" + startTime + "</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <EstimatedTimetableDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>" + startTime + "</ResponseTimestamp>\n" +
                "            <EstimatedJourneyVersionFrame>\n" +
                "                <RecordedAtTime>" + startTime + "</RecordedAtTime>\n" +
                "                <EstimatedVehicleJourney>\n" +
                "                    <RecordedAtTime>" + startTime + "</RecordedAtTime>\n" +
                "                    <LineRef>" + lineRefValue + "</LineRef>\n" +
                "                    <DirectionRef>0</DirectionRef>\n" +
                "                    <DatedVehicleJourneyRef>" + datedServiceJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    <VehicleMode>bus</VehicleMode>\n" +
                "                    <OperatorRef>" + datasource + ":Operator:123</OperatorRef>\n" +
                "                    <Monitored>true</Monitored>\n" +
                "                    <DataSource>" + datasource + "</DataSource>\n" +
                "                    <EstimatedCalls>\n" +
                "                        <EstimatedCall>\n" +
                "                            <StopPointRef>TST:Quay:001</StopPointRef>\n" +
                "                            <Order>1</Order>\n" +
                "                            <AimedDepartureTime>" + aimedTime + "</AimedDepartureTime>\n" +
                "                            <ExpectedDepartureTime>" + expectedTime + "</ExpectedDepartureTime>\n" +
                "                            <DepartureStatus>delayed</DepartureStatus>\n" +
                "                        </EstimatedCall>\n" +
                "                    </EstimatedCalls>\n" +
                "                    <IsCompleteStopSequence>true</IsCompleteStopSequence>\n" +
                "                </EstimatedVehicleJourney>\n" +
                "            </EstimatedJourneyVersionFrame>\n" +
                "        </EstimatedTimetableDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";

        SiriRecord siri = createSiriRecord(xml);
        assertNotNull(siri);

        EstimatedVehicleJourneyRecord evj = siri.getServiceDelivery()
                .getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0);

        // Call the mapper directly to isolate the !isEmpty() guard behaviour.
        // (Going through convertSiriEtToGtfsRt would also work, but the TripUpdate is serialized
        // to bytes there; direct mapper access gives a cleaner assertion.)
        GtfsRtMapper mapper = new GtfsRtMapper(NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE, serviceJourneyService);
        GtfsRealtime.TripUpdate.Builder tripUpdate = mapper.mapTripUpdateFromVehicleJourney(evj);

        assertFalse(tripUpdate.hasTrip(),
                "TripDescriptor must NOT be set when getTripId() would return an empty string; " +
                "the !isEmpty() guard prevents setTrip() from being called with an empty TripDescriptor");
    }
}
