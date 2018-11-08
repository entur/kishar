package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
import uk.org.siri.siri20.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;

public class TestSiriVMToGtfsRealtimeService {
    SiriToGtfsRealtimeService rtService;

    @Before
    public void init() {
        rtService = new SiriToGtfsRealtimeService(new AlertFactory());
    }

    @Test
    public void testVmToVehiclePosition() throws IOException {

        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String vehicleRefValue = "TST:Vehicle:1234";

        Siri siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue);

        rtService.processDelivery(siri);
        rtService.writeOutput();

        Object vehiclePositions = rtService.getVehiclePositions("application/json");
        assertNotNull(vehiclePositions);
        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);


        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getVehiclePositions(null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();
        assertNotNull(vehiclePosition);

        assertEquals(datedVehicleJourneyRef, vehiclePosition.getTrip().getTripId());
        assertEquals(lineRefValue, vehiclePosition.getTrip().getRouteId());
        assertEquals(vehicleRefValue, vehiclePosition.getVehicle().getId());
        assertNotNull(vehiclePosition.getPosition());
        assertEquals((float)latitude, vehiclePosition.getPosition().getLatitude());
        assertEquals((float)longitude, vehiclePosition.getPosition().getLongitude());
    }

    @Test
    public void testVmWithoutFramedVehicleRef() throws IOException {
        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = null;
        String vehicleRefValue = "TST:Vehicle:1234";

        Siri siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue);
        rtService.processDelivery(siri);
        rtService.writeOutput();

        Object vehiclePositions = rtService.getVehiclePositions("application/json");
        assertNotNull(vehiclePositions);
        assertTrue(vehiclePositions instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

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

    private Siri createSiriVmDelivery(String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef, String vehicleRefValue) {
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

        activity.setMonitoredVehicleJourney(mvj);
        activity.setRecordedAtTime(ZonedDateTime.now());
        vmDelivery.getVehicleActivities().add(activity);
        serviceDelivery.getVehicleMonitoringDeliveries().add(vmDelivery);
        return siri;
    }
}
