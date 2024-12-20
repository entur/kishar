package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.LocationRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.jupiter.api.Test;
import uk.org.siri.siri21.OccupancyEnumeration;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class TestSiriVMToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest{

    @Test
    public void testIncomingAtPercentageVmToVehiclePosition() throws IOException {

        String stopPointRefValue = "TST:Quay:1234";
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasource = "TST";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnumeration occupancy = OccupancyEnumeration.FULL;
        int progressPercentage = NEXT_STOP_PERCENTAGE + 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.VEHICLE_POSITION);
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
        String datasource = "TST";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnumeration occupancy = OccupancyEnumeration.SEATS_AVAILABLE;
        int progressPercentage = 51;
        int distance = NEXT_STOP_DISTANCE*2;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.VEHICLE_POSITION);
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
        String datasource = "TST";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnumeration occupancy = OccupancyEnumeration.STANDING_AVAILABLE;
        int progressPercentage = NEXT_STOP_PERCENTAGE - 1;
        int distance = 10000;

        boolean isVehicleAtStop = false;

        SiriRecord siri = createSiriVmDelivery(stopPointRefValue, lineRefValue, latitude, longitude,
                datedVehicleJourneyRef, vehicleRefValue, datasource, bearing,
                velocity, occupancy, progressPercentage, distance, isVehicleAtStop);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.VEHICLE_POSITION);
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
        String datasource1 = "TST";
        String datasource2 = "BNR";

        SiriRecord siriTST = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef1, vehicleRefValue, datasource1);
        SiriRecord siriBNR = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef2, vehicleRefValue, datasource2);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siriTST), RedisService.Type.VEHICLE_POSITION);
        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siriBNR), RedisService.Type.VEHICLE_POSITION);
        rtService.writeOutput();

        Object vehiclePositions = rtService.getVehiclePositions("application/json", "TST");
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
        String datasource = "TST";

        SiriRecord siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasource);

        redisService.writeGtfsRt(rtService.convertSiriToGtfsRt(siri), RedisService.Type.VEHICLE_POSITION);

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
        String datasource = "TST";

        float bearing = 123.45F;
        long velocity = 56;
        OccupancyEnumeration occupancy = OccupancyEnumeration.FULL;
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

        ZonedDateTime now = ZonedDateTime.now();
        String vmXml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <MoreData>true</MoreData>\n" +
                "        <VehicleMonitoringDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</ResponseTimestamp>\n" +
                "            <VehicleActivity>\n" +
                "                <RecordedAtTime>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</RecordedAtTime>\n" +
                "                <ValidUntilTime>" + now.plusMinutes(10).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</ValidUntilTime>\n" +
                "                <MonitoredVehicleJourney>\n" +
                "                    <LineRef>" + lineRefValue + "</LineRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2024-12-20</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>" + datedVehicleJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <VehicleMode>bus</VehicleMode>\n" +
                "                    <OperatorRef>309</OperatorRef>\n" +
                "                    <Monitored>true</Monitored>\n" +
                "                    <DataSource>" + datasource + "</DataSource>\n" +
                "                    <VehicleLocation>\n" +
                "                        <Longitude>" + longitude + "</Longitude>\n" +
                "                        <Latitude>" + latitude + "</Latitude>\n" +
                "                    </VehicleLocation>\n" +
                "                    <VehicleStatus>inProgress</VehicleStatus>\n" +
                "                    <VehicleRef>" + vehicleRefValue + "</VehicleRef>\n" +
                "                    <IsCompleteStopSequence>false</IsCompleteStopSequence>\n" +
                "                </MonitoredVehicleJourney>\n" +
                "            </VehicleActivity>\n" +
                "        </VehicleMonitoringDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";


        return createSiriRecord(vmXml);
    }


    private SiriRecord createSiriVmDelivery(String stopPointRef, String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef,
                                            String vehicleRefValue, String datasource, float bearing, long velocity,
                                            OccupancyEnumeration occupancy, double progressPercentage, double distance, boolean isVehicleAtStop) {

        ZonedDateTime now = ZonedDateTime.now();
        String vmXml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <MoreData>true</MoreData>\n" +
                "        <VehicleMonitoringDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</ResponseTimestamp>\n" +
                "            <VehicleActivity>\n" +
                "                <RecordedAtTime>" + now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</RecordedAtTime>\n" +
                "                <ValidUntilTime>" + now.plusMinutes(10).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +"</ValidUntilTime>\n" +
                "                <ProgressBetweenStops>\n" +
                "                    <LinkDistance>" + distance + "</LinkDistance>\n" +
                "                    <Percentage>" + progressPercentage + "</Percentage>\n" +
                "                </ProgressBetweenStops>\n" +
                "                <MonitoredVehicleJourney>\n" +
                "                    <LineRef>" + lineRefValue + "</LineRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2024-12-20</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>" + datedVehicleJourneyRef + "</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <VehicleMode>bus</VehicleMode>\n" +
                "                    <OperatorRef>309</OperatorRef>\n" +
                "                    <Monitored>true</Monitored>\n" +
                "                    <Occupancy>" + occupancy.value() + "</Occupancy>\n" +
                "                    <DataSource>" + datasource + "</DataSource>\n" +
                "                    <VehicleLocation>\n" +
                "                        <Longitude>" + longitude + "</Longitude>\n" +
                "                        <Latitude>" + latitude + "</Latitude>\n" +
                "                    </VehicleLocation>\n" +
                "                    <Bearing>" + bearing + "</Bearing>\n" +
                "                    <Velocity>" + velocity + "</Velocity>\n" +
                "                    <VehicleStatus>inProgress</VehicleStatus>\n" +
                "                    <VehicleRef>" + vehicleRefValue + "</VehicleRef>\n" +
                "                    <MonitoredCall>\n" +
                "                        <StopPointRef>" + stopPointRef + "</StopPointRef>\n" +
                "                        <VehicleAtStop>" + isVehicleAtStop + "</VehicleAtStop>\n" +
                "                    </MonitoredCall>\n" +
                "                    <IsCompleteStopSequence>false</IsCompleteStopSequence>\n" +
                "                </MonitoredVehicleJourney>\n" +
                "            </VehicleActivity>\n" +
                "        </VehicleMonitoringDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";


        return createSiriRecord(vmXml);
    }
}
