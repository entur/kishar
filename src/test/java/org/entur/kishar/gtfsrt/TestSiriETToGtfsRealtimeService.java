package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);


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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);


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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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

            assertEquals(i, stopTimeUpdate.getStopSequence());

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
    public void testEtToTripUpdateFilterOnDatasource() throws IOException {
        // Specifying local service for specific datasource-testing
//        SiriToGtfsRealtimeService rtService = new SiriToGtfsRealtimeService(new AlertFactory(), redisService,
//                serviceJourneyService,
//                Lists.newArrayList("TST", "BNR"), Lists.newArrayList(),
//                Lists.newArrayList(), NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE);

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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        assertEquals(2, entityList.size());
    }

    @Test
    public void testEtToTripUpdateNoWhitelist() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "TST";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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
    public void testEtToTripUpdateIgnoreDatasourceNotInWhitelist() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "NSB";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, 1, delayPerStop, datedVehicleJourneyRef, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.TRIP_UPDATE);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());
    }

    @Test
    public void testEtWithoutFramedVehicleRef() throws IOException {
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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

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
}
