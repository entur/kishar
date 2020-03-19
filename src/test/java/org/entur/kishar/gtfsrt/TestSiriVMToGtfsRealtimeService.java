package org.entur.kishar.gtfsrt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.App;
import org.entur.kishar.gtfsrt.domain.VehiclePositionKey;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.org.siri.siri20.LocationStructure;
import uk.org.siri.siri20.MonitoredCallStructure;
import uk.org.siri.siri20.OccupancyEnumeration;
import uk.org.siri.siri20.ProgressBetweenStopsStructure;
import uk.org.siri.siri20.ServiceDelivery;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.StopPointRef;
import uk.org.siri.siri20.VehicleActivityStructure;
import uk.org.siri.siri20.VehicleMonitoringDeliveryStructure;
import uk.org.siri.siri20.VehicleRef;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.ArrayList;
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        Siri siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(datasource, siri);

        when(redisService.readAllVehiclePositions()).thenReturn(redisMap);
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

    private Map<byte[], byte[]> getRedisMap(String datasource, Siri siri) throws IOException {
        List<GtfsRealtime.FeedEntity> gtfsRt = rtService.convertSiriVmToGtfsRt(siri);
        Map<byte[], byte[]> redisMap = Maps.newHashMap();
        for (GtfsRealtime.FeedEntity feedEntity : gtfsRt) {
            byte[] entityInBytes = feedEntity.toByteArray();
            byte[] paddedEntityInBytes = new byte[entityInBytes.length + 16];
            System.arraycopy(entityInBytes, 0, paddedEntityInBytes, 16, entityInBytes.length);
            redisMap.put(new VehiclePositionKey(feedEntity.getId(), datasource).toByteArray(), paddedEntityInBytes);
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.SEATS_AVAILABLE;
        int progressPercentage = 51;
        int distance = NEXT_STOP_DISTANCE*2;

        boolean isVehicleAtStop = false;

        Siri siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(datasource, siri);

        when(redisService.readAllVehiclePositions()).thenReturn(redisMap);
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.STANDING_AVAILABLE;
        int progressPercentage = NEXT_STOP_PERCENTAGE - 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        Siri siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        Map<byte[], byte[]> redisMap = getRedisMap(datasource, siri);

        when(redisService.readAllVehiclePositions()).thenReturn(redisMap);
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

        Siri siriRUT = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef1, vehicleRefValue, datasource1);
        Siri siriBNR = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef2, vehicleRefValue, datasource2);

        Map<byte[], byte[]> redisMap = getRedisMap(datasource1, siriRUT);
        Map<byte[], byte[]> siriBnrMap = getRedisMap(datasource2, siriBNR);

        redisMap.putAll(siriBnrMap);

        when(redisService.readAllVehiclePositions()).thenReturn(redisMap);
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

        Siri siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasource);

        Map<byte[], byte[]> redisMap = getRedisMap(datasource, siri);

        when(redisService.readAllVehiclePositions()).thenReturn(redisMap);
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
        OccupancyEnumeration occupancy = OccupancyEnumeration.FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        Siri siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        List<GtfsRealtime.FeedEntity> result = rtService.convertSiriVmToGtfsRt(siri);

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

    private VehicleRef createVehicleRef(String value) {
        VehicleRef ref = new VehicleRef();
        ref.setValue(value);
        return ref;
    }

    private LocationStructure createLocation(double longitude, double latitude) {
        LocationStructure loc = new LocationStructure();
        loc.setLongitude(BigDecimal.valueOf(longitude));
        loc.setLatitude(BigDecimal.valueOf(latitude));
        return loc;
    }

    private Siri createSiriVmDelivery(String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef, String vehicleRefValue, String datasource) {
        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        siri.setServiceDelivery(serviceDelivery);
        VehicleMonitoringDeliveryStructure vmDelivery = new VehicleMonitoringDeliveryStructure();
        VehicleActivityStructure activity = new VehicleActivityStructure();
        VehicleActivityStructure.MonitoredVehicleJourney mvj = new VehicleActivityStructure.MonitoredVehicleJourney();

        mvj.setLineRef(createLineRef(lineRefValue));

        mvj.setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));

        mvj.setVehicleLocation(createLocation(longitude, latitude));

        mvj.setVehicleRef(createVehicleRef(vehicleRefValue));

        mvj.setDataSource(datasource);

        activity.setMonitoredVehicleJourney(mvj);
        activity.setRecordedAtTime(ZonedDateTime.now());
        activity.setValidUntilTime(ZonedDateTime.now().plusMinutes(10));
        vmDelivery.getVehicleActivities().add(activity);
        serviceDelivery.getVehicleMonitoringDeliveries().add(vmDelivery);
        return siri;
    }


    private Siri createSiriVmDelivery(String stopPointRefValue, String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef,
                                      String vehicleRefValue, String datasource, float bearing, long velocity,
                                      OccupancyEnumeration occupancy, int progressPercentage, int distance, boolean isVehicleAtStop) {
        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        siri.setServiceDelivery(serviceDelivery);
        VehicleMonitoringDeliveryStructure vmDelivery = new VehicleMonitoringDeliveryStructure();
        VehicleActivityStructure activity = new VehicleActivityStructure();

        ProgressBetweenStopsStructure progress = new ProgressBetweenStopsStructure();
        progress.setPercentage(BigDecimal.valueOf(progressPercentage));
        progress.setLinkDistance(BigDecimal.valueOf(distance));
        activity.setProgressBetweenStops(progress);

        VehicleActivityStructure.MonitoredVehicleJourney mvj = new VehicleActivityStructure.MonitoredVehicleJourney();

        mvj.setLineRef(createLineRef(lineRefValue));
        mvj.setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        mvj.setVehicleLocation(createLocation(longitude, latitude));
        mvj.setVehicleRef(createVehicleRef(vehicleRefValue));

        mvj.setBearing(Float.valueOf(bearing));
        mvj.setVelocity(BigInteger.valueOf(velocity));
        mvj.setOccupancy(occupancy);
        mvj.setDataSource(datasource);

        MonitoredCallStructure monitoredCall = new MonitoredCallStructure();
        monitoredCall.setVehicleAtStop(isVehicleAtStop);
        StopPointRef stopPointRef = new StopPointRef();
        stopPointRef.setValue(stopPointRefValue);
        monitoredCall.setStopPointRef(stopPointRef);
        mvj.setMonitoredCall(monitoredCall);


        activity.setMonitoredVehicleJourney(mvj);
        activity.setRecordedAtTime(ZonedDateTime.now());
        activity.setValidUntilTime(ZonedDateTime.now().plusMinutes(10));
        vmDelivery.getVehicleActivities().add(activity);
        serviceDelivery.getVehicleMonitoringDeliveries().add(vmDelivery);
        return siri;
    }
}
