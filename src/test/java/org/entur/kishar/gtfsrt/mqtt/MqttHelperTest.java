package org.entur.kishar.gtfsrt.mqtt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.routes.helpers.MqttHelper;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class MqttHelperTest {

    @Test
    public void testTopicFormat() {

        GtfsRealtime.VehiclePosition.Builder vp = GtfsRealtime.VehiclePosition.newBuilder();

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();
        td.setTripId("ENT:ServiceJourney:1234");
        td.setRouteId("ENT:Line:1234");
        td.setStartDate("2020-02-01");
        td.setStartTime("12:34:56");
        td.setDirectionId(1);
        vp.setTrip(td);

        vp.setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId("vehicle-123"));

        vp.setStopId("NSR:StopPlace:123");

        vp.setPosition(GtfsRealtime.Position.newBuilder().setLatitude(59.1234F).setLongitude(10.1234F));

        final String topic = MqttHelper.buildTopic(vp.build());

        assertEquals("/gtfsrt/ENT:Line:1234/ENT:ServiceJourney:1234/1/12:34:56/NSR:StopPlace:123/59;10/11/22/33/", topic);
    }
}
