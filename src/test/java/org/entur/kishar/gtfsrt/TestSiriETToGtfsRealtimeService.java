package org.entur.kishar.gtfsrt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.junit.Test;
import uk.org.siri.www.siri.*;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createFramedVehicleJourneyRefStructure;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;
import static org.mockito.Mockito.when;

public class TestSiriETToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testAsyncGtfsRtProduction() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        SiriType siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

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

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriType siri) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriEtToGtfsRt(siri);
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

        SiriType siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

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
        SiriToGtfsRealtimeService localRtService = new SiriToGtfsRealtimeService(new AlertFactory(), redisService,
                Lists.newArrayList("RUT", "BNR"), Lists.newArrayList(),
                Lists.newArrayList(), NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE);

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef1 = "TST:ServiceJourney:1234";
        String datedVehicleJourneyRef2 = "TST:ServiceJourney:1235";
        String datasource1 = "RUT";
        String datasource2 = "BNR";

        SiriType siriRUT = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef1, datasource1);
        SiriType siriBNR = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef2, datasource2);

        Map<String, byte[]> redisMap = getRedisMap(localRtService, siriRUT);
        Map<String, byte[]> siriBnrMap = getRedisMap(localRtService, siriBNR);

        redisMap.putAll(siriBnrMap);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
        localRtService.writeOutput();

        Object tripUpdates = localRtService.getTripUpdates("application/json", "RUT");
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

        tripUpdates = localRtService.getTripUpdates("application/json", "BNR");
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

        tripUpdates = localRtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        assertEquals(2, entityList.size());
    }

    @Test
    public void testEtToTripUpdateNoWhitelist() throws IOException {
        SiriToGtfsRealtimeService localRtService = new SiriToGtfsRealtimeService(new AlertFactory(), redisService, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE);

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        SiriType siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, byte[]> redisMap = getRedisMap(localRtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
        localRtService.writeOutput();

        Object tripUpdates = localRtService.getTripUpdates("application/json", null);
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

        SiriType siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

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
        SiriType siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef, datasource);
        assertFalse(siri.getServiceDelivery().getEstimatedTimetableDeliveryList().get(0)
                .getEstimatedJourneyVersionFrameList().get(0)
                .getEstimatedVehicleJourneyList().get(0)
                .hasFramedVehicleJourneyRef());

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

        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveryList().get(0)
                .getEstimatedJourneyVersionFrameList().get(0)
                .getEstimatedVehicleJourneyList().get(0)
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

        SiriType et = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, delayPerStop), datedVehicleJourneyRef, datasource);

        Map<String, GtfsRtData> result = rtService.convertSiriEtToGtfsRt(et);

        assertFalse(result.isEmpty());
    }

    private SiriType createSiriEtDelivery(String lineRefValue, List<? extends EstimatedCallStructure> calls, String datedVehicleJourneyRef, String datasource) {

        EstimatedVehicleJourneyStructure.EstimatedCallsType estimatedCalls = EstimatedVehicleJourneyStructure.EstimatedCallsType.newBuilder()
                .addAllEstimatedCall(calls)
                .build();

        EstimatedVehicleJourneyStructure.Builder estimatedVehicleJourneyBuilder = EstimatedVehicleJourneyStructure.newBuilder()
                .setLineRef(createLineRef(lineRefValue))
                .setDataSource(datasource)
                .setEstimatedCalls(estimatedCalls);

        if (datedVehicleJourneyRef != null) {
            estimatedVehicleJourneyBuilder.setFramedVehicleJourneyRef(createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        EstimatedVehicleJourneyStructure estimatedVehicleJourney = estimatedVehicleJourneyBuilder.build();

        EstimatedVersionFrameStructure etVersionFrame = EstimatedVersionFrameStructure.newBuilder()
                .addEstimatedVehicleJourney(estimatedVehicleJourney)
                .build();

        EstimatedTimetableDeliveryStructure etDelivery = EstimatedTimetableDeliveryStructure.newBuilder()
                .addEstimatedJourneyVersionFrame(etVersionFrame)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addEstimatedTimetableDelivery(etDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }


    private List<? extends EstimatedCallStructure> createEstimatedCalls(int stopCount, int addedDelayPerStop) {
        List<EstimatedCallStructure> calls = new ArrayList<>();
        Timestamp startTime = SiriLibrary.getCurrentTime();

        for (int i = 0; i < stopCount; i++) {
            StopPointRefStructure stopPointRef = StopPointRefStructure.newBuilder()
                    .setValue("TST:Quay:1234-" + i)
                    .build();


            EstimatedCallStructure.Builder call = EstimatedCallStructure.newBuilder()
                    .setStopPointRef(stopPointRef);

            startTime = Timestamps.add(startTime, Duration.newBuilder().setSeconds(60).build());
            if (i > 0) {
                call.setAimedArrivalTime(startTime);
                Timestamp expected = Timestamps.add(startTime, Duration.newBuilder().setSeconds(addedDelayPerStop).build());
                call.setExpectedArrivalTime(expected);
            }
            if (i < stopCount-1) {
                call.setAimedDepartureTime(startTime);
                call.setExpectedDepartureTime(Timestamps.add(startTime, Duration.newBuilder().setSeconds(addedDelayPerStop).build()));
            }
            calls.add(call.build());
        }
        return calls;
    }
}
