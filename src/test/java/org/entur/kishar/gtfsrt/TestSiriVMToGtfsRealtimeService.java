package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;
import uk.org.siri.www.siri.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.OCCUPANCY_ENUMERATION_FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriType siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(siri);

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

    private Map<byte[], byte[]> getRedisMap(SiriType siri) {
        Map<byte[], byte[]> gtfsRt = rtService.convertSiriVmToGtfsRt(siri);
        Map<byte[], byte[]> redisMap = Maps.newHashMap();
        for (byte[] key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key);
            byte[] dataInBytes = new byte[data.length + 16];
            System.arraycopy(data, 0, dataInBytes, 16, data.length);
            redisMap.put(key, dataInBytes);
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.OCCUPANCY_ENUMERATION_SEATS_AVAILABLE;
        int progressPercentage = 51;
        int distance = NEXT_STOP_DISTANCE*2;

        boolean isVehicleAtStop = false;

        SiriType siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(siri);

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
        OccupancyEnumeration occupancy = OccupancyEnumeration.OCCUPANCY_ENUMERATION_STANDING_AVAILABLE;
        int progressPercentage = NEXT_STOP_PERCENTAGE - 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriType siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(siri);

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

        SiriType siriRUT = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef1, vehicleRefValue, datasource1);
        SiriType siriBNR = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef2, vehicleRefValue, datasource2);

        Map<byte[], byte[]> redisMap = getRedisMap(siriRUT);
        Map<byte[], byte[]> siriBnrMap = getRedisMap(siriBNR);

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

        SiriType siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasource);

        Map<byte[], byte[]> redisMap = getRedisMap(siri);

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
        OccupancyEnumeration occupancy = OccupancyEnumeration.OCCUPANCY_ENUMERATION_FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriType siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> result = rtService.convertSiriVmToGtfsRt(siri);

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

    private VehicleRefStructure createVehicleRef(String value) {
        return VehicleRefStructure.newBuilder()
                .setValue(value)
                .build();
    }

    private LocationStructure createLocation(double longitude, double latitude) {
        return LocationStructure.newBuilder()
                .setLongitude(longitude)
                .setLatitude(latitude)
                .build();
    }

    private SiriType createSiriVmDelivery(String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef, String vehicleRefValue, String datasource) {

        VehicleActivityStructure.MonitoredVehicleJourneyType.Builder mvjBuilder = VehicleActivityStructure.MonitoredVehicleJourneyType.newBuilder()
                .setLineRef(createLineRef(lineRefValue))
                .setVehicleRef(createVehicleRef(vehicleRefValue))
                .setVehicleLocation(createLocation(longitude, latitude))
                .setDataSource(datasource);

        if (datedVehicleJourneyRef != null) {
            mvjBuilder.setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        VehicleActivityStructure.MonitoredVehicleJourneyType mvj = mvjBuilder.build();

        VehicleActivityStructure activity = VehicleActivityStructure.newBuilder()
                .setMonitoredVehicleJourney(mvj)
                .setRecordedAtTime(Timestamp.getDefaultInstance())
                .setValidUntilTime(Timestamps.add(Timestamp.getDefaultInstance(), Duration.newBuilder().setSeconds(600).build()))
                .build();

        VehicleMonitoringDeliveryStructure vmDelivery = VehicleMonitoringDeliveryStructure.newBuilder()
                .addVehicleActivity(activity)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addVehicleMonitoringDelivery(vmDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }


    private SiriType createSiriVmDelivery(String stopPointRefValue, String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef,
                                      String vehicleRefValue, String datasource, float bearing, long velocity,
                                      OccupancyEnumeration occupancy, int progressPercentage, int distance, boolean isVehicleAtStop) {


        StopPointRefStructure stopPointRef = StopPointRefStructure.newBuilder()
                .setValue(stopPointRefValue)
                .build();

        MonitoredCallStructure monitoredCall = MonitoredCallStructure.newBuilder()
                .setVehicleAtStop(isVehicleAtStop)
                .setStopPointRef(stopPointRef)
                .build();

        VehicleActivityStructure.MonitoredVehicleJourneyType mvj = VehicleActivityStructure.MonitoredVehicleJourneyType.newBuilder()
                .setLineRef(createLineRef(lineRefValue))
                .setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef))
                .setVehicleLocation(createLocation(longitude, latitude))
                .setVehicleRef(createVehicleRef(vehicleRefValue))
                .setBearing(bearing)
                .setVelocity((int)velocity)
                .setOccupancy(occupancy)
                .setDataSource(datasource)
                .setMonitoredCall(monitoredCall)
                .build();

        ProgressBetweenStopsStructure progress = ProgressBetweenStopsStructure.newBuilder()
                .setPercentage(progressPercentage)
                .setLinkDistance(distance)
                .build();

        VehicleActivityStructure activity = VehicleActivityStructure.newBuilder()
                .setProgressBetweenStops(progress)
                .setMonitoredVehicleJourney(mvj)
                .setRecordedAtTime(Timestamp.getDefaultInstance())
                .setValidUntilTime(Timestamps.add(Timestamp.getDefaultInstance(), Duration.newBuilder().setSeconds(600).build()))
                .build();

        VehicleMonitoringDeliveryStructure vmDelivery = VehicleMonitoringDeliveryStructure.newBuilder()
                .addVehicleActivity(activity)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addVehicleMonitoringDelivery(vmDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }
}
