package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProviderImpl;
import uk.org.siri.siri20.*;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createFramedVehicleJourneyRefStructure;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;

public class TestSiriETToGtfsRealtimeService {
    SiriToGtfsRealtimeService rtService;

    @Before
    public void init() {
        rtService = new SiriToGtfsRealtimeService(new AlertFactory());
    }

    @Test
    public void testAsyncGtfsRtProduction() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef);

        rtService.processDelivery(siri);

        // GTFS-RT is produced asynchronously - should be empty at first

        Object tripUpdates = rtService.getTripUpdates("application/json");
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json");
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);


        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null));
        assertEquals(feedMessage, byteArrayFeedMessage);

    }

    @Test
    public void testEtToTripUpdate() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef);

        rtService.processDelivery(siri);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json");
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null));
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
    public void testEtWithoutFramedVehicleRef() throws IOException {
        String lineRefValue = "TST:Line:1234";
        String datedVehicleJourneyRef = null;

        // Assert that ET is ignored when framedVehicleRef is null
        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef);
        assertNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneies().get(0)
                .getFramedVehicleJourneyRef());
        rtService.processDelivery(siri);
        rtService.writeOutput();

        Object tripUpdates = rtService.getTripUpdates("application/json");
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());


        datedVehicleJourneyRef = "TTT:ServiceJourney:1234";
        siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef);

        assertNotNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneies().get(0)
                .getFramedVehicleJourneyRef());

        rtService.processDelivery(siri);
        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json");
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

    }

    private Siri createSiriEtDelivery(String lineRefValue, List<? extends EstimatedCall> calls, String datedVehicleJourneyRef) {
        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();
        siri.setServiceDelivery(serviceDelivery);
        EstimatedTimetableDeliveryStructure etDelivery = new EstimatedTimetableDeliveryStructure();
        serviceDelivery.getEstimatedTimetableDeliveries().add(etDelivery);
        EstimatedVersionFrameStructure etVersionFrame = new EstimatedVersionFrameStructure();
        etDelivery.getEstimatedJourneyVersionFrames().add(etVersionFrame);
        EstimatedVehicleJourney estimatedVehicleJourney = new EstimatedVehicleJourney();
        etVersionFrame.getEstimatedVehicleJourneies().add(estimatedVehicleJourney);

        estimatedVehicleJourney.setLineRef(createLineRef(lineRefValue));
        estimatedVehicleJourney.setFramedVehicleJourneyRef(createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));

        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        estimatedCalls.getEstimatedCalls().addAll(calls);
        estimatedVehicleJourney.setEstimatedCalls(estimatedCalls);
        return siri;
    }


    private List<? extends EstimatedCall> createEstimatedCalls(int stopCount, int addedDelayPerStop) {
        List<EstimatedCall> calls = new ArrayList<>();
        ZonedDateTime startTime = ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES);

        for (int i = 0; i < stopCount; i++) {
            EstimatedCall call = new EstimatedCall();
            StopPointRef stopPointRef = new StopPointRef();
            stopPointRef.setValue("TST:Quay:1234-" + i);
            call.setStopPointRef(stopPointRef);
            startTime = startTime.plusMinutes(i);

            if (i > 0) {
                call.setAimedArrivalTime(startTime);
                call.setExpectedArrivalTime(startTime.plusSeconds(addedDelayPerStop));
            }
            if (i < stopCount-1) {
                call.setAimedDepartureTime(startTime);
                call.setExpectedDepartureTime(startTime.plusSeconds(addedDelayPerStop));
            }
            calls.add(call);
        }
        return calls;
    }
}
