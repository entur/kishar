package org.entur.kishar.gtfsrt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.EstimatedCallRecord;
import org.entur.avro.realtime.siri.model.EstimatedJourneyVersionFrameRecord;
import org.entur.avro.realtime.siri.model.EstimatedTimetableDeliveryRecord;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.ServiceDeliveryRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.entur.kishar.gtfsrt.Helper.createFramedVehicleJourneyRefStructure;
import static org.mockito.Mockito.when;

public class TestSiriETToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testAsyncGtfsRtProduction() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);

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
        String datasource = "RUT";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, null), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);

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
        String datasource = "RUT";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
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
//                Lists.newArrayList("RUT", "BNR"), Lists.newArrayList(),
//                Lists.newArrayList(), NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE);

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef1 = "TST:ServiceJourney:1234";
        String datedVehicleJourneyRef2 = "TST:ServiceJourney:1235";
        String datasource1 = "RUT";
        String datasource2 = "BNR";

        SiriRecord siriRUT = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef1, datasource1);
        SiriRecord siriBNR = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef2, datasource2);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siriRUT);
        Map<String, byte[]> siriBnrMap = getRedisMap(rtService, siriBNR);

        redisMap.putAll(siriBnrMap);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", "RUT");
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
        String datasource = "RUT";

        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
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

        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
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
        String datasource = "RUT";

        // Assert that ET is ignored when framedVehicleRef is null
        SiriRecord siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef, datasource);
        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0)
                .getFramedVehicleJourneyRef());

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());


        datedVehicleJourneyRef = "TTT:ServiceJourney:1234";
        siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef, datasource);

        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneys().get(0)
                .getFramedVehicleJourneyRef());

        redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
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
        String datasource = "RUT";

        SiriRecord et = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, GtfsRtData> result = rtService.convertSiriToGtfsRt(et);

        assertFalse(result.isEmpty());
    }

    private SiriRecord createSiriEtDelivery(String lineRefValue, List<EstimatedCallRecord> calls, String datedVehicleJourneyRef, String datasource) {

        EstimatedVehicleJourneyRecord.Builder estimatedVehicleJourneyBuilder = EstimatedVehicleJourneyRecord.newBuilder()
                .setLineRef(lineRefValue)
                .setDataSource(datasource)
                .setEstimatedCalls(calls);

        if (datedVehicleJourneyRef != null) {
            estimatedVehicleJourneyBuilder.setFramedVehicleJourneyRef(createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        EstimatedVehicleJourneyRecord estimatedVehicleJourney = estimatedVehicleJourneyBuilder.build();

        EstimatedJourneyVersionFrameRecord etVersionFrame = EstimatedJourneyVersionFrameRecord.newBuilder()
                .setEstimatedVehicleJourneys(List.of(estimatedVehicleJourney))
                .build();

        EstimatedTimetableDeliveryRecord etDelivery = EstimatedTimetableDeliveryRecord.newBuilder()
                .setEstimatedJourneyVersionFrames(List.of(etVersionFrame))
                .build();

        ServiceDeliveryRecord serviceDelivery = ServiceDeliveryRecord.newBuilder()
                .setEstimatedTimetableDeliveries(List.of(etDelivery))
                .build();

        return SiriRecord.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }


    private List<EstimatedCallRecord> createEstimatedCalls(int stopCount, Integer addedDelayPerStop) {
        List<EstimatedCallRecord> calls = new ArrayList<>();
        ZonedDateTime startTime = ZonedDateTime.now();

        for (int i = 0; i < stopCount; i++) {
            String stopPointRef = "TST:Quay:1234-";

            EstimatedCallRecord.Builder call = EstimatedCallRecord.newBuilder()
                    .setStopPointRef(stopPointRef);

            startTime = startTime.plusSeconds(60);
            if (i > 0) {
                call.setAimedArrivalTime(startTime.format(DateTimeFormatter.ISO_DATE_TIME));
                if (addedDelayPerStop != null) {
                    ZonedDateTime expected = startTime.plusSeconds(addedDelayPerStop);
                    call.setExpectedArrivalTime(expected.format(DateTimeFormatter.ISO_DATE_TIME));
                }
            }
            if (i < stopCount-1) {
                call.setAimedDepartureTime(startTime.format(DateTimeFormatter.ISO_DATE_TIME));

                if (addedDelayPerStop != null) {
                    ZonedDateTime expected = startTime.plusSeconds(addedDelayPerStop);
                    call.setExpectedDepartureTime(expected.format(DateTimeFormatter.ISO_DATE_TIME));
                }
            }
            calls.add(call.build());
        }
        return calls;
    }
}
