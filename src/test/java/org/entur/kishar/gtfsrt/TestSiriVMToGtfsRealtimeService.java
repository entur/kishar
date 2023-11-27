package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.CallRecord;
import org.entur.avro.realtime.siri.model.LocationRecord;
import org.entur.avro.realtime.siri.model.MonitoredVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.OccupancyEnum;
import org.entur.avro.realtime.siri.model.ProgressBetweenStopsRecord;
import org.entur.avro.realtime.siri.model.ServiceDeliveryRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.avro.realtime.siri.model.VehicleMonitoringDeliveryRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.when;

public class TestSiriVMToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest{

    @Test
    public void testIncomingAtPercentageVmToVehiclePosition() throws IOException {

        String stopPointRefValue = "TST:Quay:1234";
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "RUT";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnum occupancy = OccupancyEnum.FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        rtService.writeOutput();

        GtfsRealtime.FeedMessage feedMessage = getFeedMessage(rtService);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
        assertNotNull(vehiclePosition);

        assertEquals(datedVehicleJourneyRef, vehiclePosition.getTrip().getTripId());
        assertEquals(lineRefValue, vehiclePosition.getTrip().getRouteId());
        assertEquals(vehicleRefValue, vehiclePosition.getVehicle().getId());

        assertEquals(stopPointRefValue, vehiclePosition.getStopId());

        assertEquals(GtfsRealtime.VehiclePosition.OccupancyStatus.FULL, vehiclePosition.getOccupancyStatus());
        assertEquals(GtfsRealtime.VehiclePosition.VehicleStopStatus.INCOMING_AT, vehiclePosition.getCurrentStatus());

        final GtfsRealtime.Position position = vehiclePosition.getPosition();
        assertNotNull(position);
        assertEquals((float)latitude, position.getLatitude());
        assertEquals((float)longitude, position.getLongitude());

        assertEquals(bearing, position.getBearing());
        assertEquals((float)velocity, position.getSpeed());
    }

    private Map<String, byte[]> getRedisMap(
        SiriToGtfsRealtimeService realtimeService, SiriRecord siri
    ) {
        Map<String, GtfsRtData> gtfsRt = realtimeService.convertSiriToGtfsRt(siri);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    @Test
    public void testIncomingAtDistanceVmToVehiclePosition() throws IOException {

        String stopPointRefValue = "TST:Quay:1234";
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "RUT";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnum occupancy = OccupancyEnum.SEATS_AVAILABLE;
        int progressPercentage = 51;
        int distance = NEXT_STOP_DISTANCE*2;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        rtService.writeOutput();

        GtfsRealtime.FeedMessage feedMessage = getFeedMessage(rtService);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
        assertNotNull(vehiclePosition);

        assertEquals(GtfsRealtime.VehiclePosition.OccupancyStatus.FEW_SEATS_AVAILABLE, vehiclePosition.getOccupancyStatus());
        assertEquals(GtfsRealtime.VehiclePosition.VehicleStopStatus.INCOMING_AT, vehiclePosition.getCurrentStatus());

    }

    @Test
    public void testInTransitVmToVehiclePosition() throws IOException {

        String stopPointRefValue = "TST:Quay:1234";
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "RUT";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnum occupancy = OccupancyEnum.STANDING_AVAILABLE;
        int progressPercentage = NEXT_STOP_PERCENTAGE - 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        rtService.writeOutput();

        GtfsRealtime.FeedMessage feedMessage = getFeedMessage(rtService);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
        assertNotNull(vehiclePosition);

        assertEquals(GtfsRealtime.VehiclePosition.OccupancyStatus.STANDING_ROOM_ONLY, vehiclePosition.getOccupancyStatus());
        assertEquals(GtfsRealtime.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO, vehiclePosition.getCurrentStatus());

    }

    @Test
    public void testVmToVehiclePositionWithDatasourceFiltering() throws IOException {

        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef1 = "TST:ServiceJourney:1234";
        String datedVehicleJourneyRef2 = "TST:ServiceJourney:1235";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource1 = "RUT";
        String datasource2 = "BNR";

        SiriRecord siriRUT = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef1, vehicleRefValue, datasource1);
        SiriRecord siriBNR = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef2, vehicleRefValue, datasource2);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siriRUT);
        Map<String, byte[]> siriBnrMap = getRedisMap(rtService, siriBNR);

        redisMap.putAll(siriBnrMap);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        rtService.writeOutput();

        Object vehiclePositions = rtService.getVehiclePositions("application/json", null);
        assertNotNull(vehiclePositions);
        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);


        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());
        assertEquals(1, entityList.size());

        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);
        assertEquals(1, ((GtfsRealtime.FeedMessage) vehiclePositions).getEntityCount());
        assertTrue(entityList.contains(((GtfsRealtime.FeedMessage) vehiclePositions).getEntity(0)));

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getVehiclePositions(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
        assertNotNull(vehiclePosition);

        assertEquals(datedVehicleJourneyRef1, vehiclePosition.getTrip().getTripId());
    }

    @Test
    public void testVmWithoutFramedVehicleRef() throws IOException {

        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = null;
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "RUT";

        SiriRecord siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasource);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        rtService.writeOutput();

        Object vehiclePositions = rtService.getVehiclePositions("application/json", null);
        assertNotNull(vehiclePositions);
        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();


        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);
        assertEquals(0, ((GtfsRealtime.FeedMessage) vehiclePositions).getEntityCount());

        assertTrue(entityList.isEmpty());
    }

    @Test
    public void testMappingOfSiriVm() {

        String stopPointRefValue = "TST:Quay:1234";
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "RUT";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnum occupancy = OccupancyEnum.FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<String, GtfsRtData> result = rtService.convertSiriToGtfsRt(siri);

        assertFalse(result.isEmpty());
    }

    private GtfsRealtime.FeedMessage getFeedMessage(SiriToGtfsRealtimeService rtService) throws InvalidProtocolBufferException {
        Object vehiclePositions = rtService.getVehiclePositions("application/json", null);
        assertNotNull(vehiclePositions);
        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);


        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getVehiclePositions(null, null));
        assertEquals(feedMessage, byteArrayFeedMessage);
        return feedMessage;
    }

    private LocationRecord createLocation(double longitude, double latitude) {
        return LocationRecord.newBuilder()
                .setLongitude(longitude)
                .setLatitude(latitude)
                .build();
    }

    private SiriRecord createSiriVmDelivery(String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef, String vehicleRefValue, String datasource) {

        MonitoredVehicleJourneyRecord.Builder mvjBuilder = MonitoredVehicleJourneyRecord.newBuilder()
                .setLineRef(lineRefValue)
                .setVehicleRef(vehicleRefValue)
                .setVehicleLocation(createLocation(longitude, latitude))
                .setDataSource(datasource);

        if (datedVehicleJourneyRef != null) {
            mvjBuilder.setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        MonitoredVehicleJourneyRecord mvj = mvjBuilder.build();

        VehicleActivityRecord activity = VehicleActivityRecord.newBuilder()
                .setMonitoredVehicleJourney(mvj)
                .setRecordedAtTime(SiriLibrary.getCurrentTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .setValidUntilTime(SiriLibrary.getCurrentTime().plusMinutes(10).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .build();

        VehicleMonitoringDeliveryRecord vmDelivery = VehicleMonitoringDeliveryRecord.newBuilder()
                .setVehicleActivities(List.of(activity))
                .build();

        ServiceDeliveryRecord serviceDelivery = ServiceDeliveryRecord.newBuilder()
                .setVehicleMonitoringDeliveries(List.of(vmDelivery))
                .build();

        return SiriRecord.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }


    private SiriRecord createSiriVmDelivery(String stopPointRef, String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef,
                                      String vehicleRefValue, String datasource, float bearing, long velocity,
                                      OccupancyEnum occupancy, double progressPercentage, double distance, boolean isVehicleAtStop) {



        CallRecord monitoredCall = CallRecord.newBuilder()
                .setVehicleAtStop(isVehicleAtStop)
                .setStopPointRef(stopPointRef)
                .build();

        MonitoredVehicleJourneyRecord mvj = MonitoredVehicleJourneyRecord.newBuilder()
                                .setLineRef(lineRefValue)
                                .setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef))
                                .setVehicleLocation(createLocation(longitude, latitude))
                                .setVehicleRef(vehicleRefValue)
                                .setBearing(bearing)
                                .setVelocity((int)velocity)
                                .setOccupancy(occupancy)
                                .setDataSource(datasource)
                                .setMonitoredCall(monitoredCall)
                                .build();

        ProgressBetweenStopsRecord progress = ProgressBetweenStopsRecord.newBuilder()
                .setPercentage(progressPercentage)
                .setLinkDistance(distance)
                .build();

        VehicleActivityRecord activity = VehicleActivityRecord.newBuilder()
                .setProgressBetweenStops(progress)
                .setMonitoredVehicleJourney(mvj)
                .setRecordedAtTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .setValidUntilTime(ZonedDateTime.now().plusMinutes(10).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .build();

        VehicleMonitoringDeliveryRecord vmDelivery = VehicleMonitoringDeliveryRecord.newBuilder()
                .setVehicleActivities(List.of(activity))
                .build();

        ServiceDeliveryRecord serviceDelivery = ServiceDeliveryRecord.newBuilder()
                .setVehicleMonitoringDeliveries(List.of(vmDelivery))
                .build();

        return SiriRecord.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }
}
