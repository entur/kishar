package org.entur.kishar.gtfsrt;

import com.google.common.collect.Lists;
import com.google.transit.realtime.GtfsRealtime;
import org.junit.Before;
import org.junit.Test;
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
        rtService = new SiriToGtfsRealtimeService(new AlertFactory(), Lists.newArrayList("RUT", "BNR"));
    }

    @Test
    public void testAsyncGtfsRtProduction() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

        rtService.processDelivery(siri);

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
    public void testEtToTripUpdate() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, datasource);

        rtService.processDelivery(siri);
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

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef1 = "TST:ServiceJourney:1234";
        String datedVehicleJourneyRef2 = "TST:ServiceJourney:1235";
        String datasource1 = "RUT";
        String datasource2 = "BNR";

        Siri siriRUT = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef1, datasource1);
        Siri siriBNR = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef2, datasource2);

        rtService.processDelivery(siriRUT);
        rtService.processDelivery(siriBNR);
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
        rtService = new SiriToGtfsRealtimeService(new AlertFactory(), null);

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "RUT";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

        rtService.processDelivery(siri);
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

        init();
    }

    @Test
    public void testEtToTripUpdateIgnoreDatasourceNotInWhitelist() throws IOException {

        String lineRefValue = "TST:Line:1234";
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String datasource = "NSB";

        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(1, delayPerStop), datedVehicleJourneyRef, datasource);

        rtService.processDelivery(siri);
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
        Siri siri = createSiriEtDelivery(lineRefValue, createEstimatedCalls(5, 30), datedVehicleJourneyRef, datasource);
        assertNull(siri.getServiceDelivery().getEstimatedTimetableDeliveries().get(0)
                .getEstimatedJourneyVersionFrames().get(0)
                .getEstimatedVehicleJourneies().get(0)
                .getFramedVehicleJourneyRef());
        rtService.processDelivery(siri);
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
                .getEstimatedVehicleJourneies().get(0)
                .getFramedVehicleJourneyRef());

        rtService.processDelivery(siri);
        rtService.writeOutput();

        tripUpdates = rtService.getTripUpdates("application/json", null);
        assertNotNull(tripUpdates);
        assertTrue(tripUpdates instanceof GtfsRealtime.FeedMessage);

        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

    }

    private Siri createSiriEtDelivery(String lineRefValue, List<? extends EstimatedCall> calls, String datedVehicleJourneyRef, String datasource) {
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
        estimatedVehicleJourney.setDataSource(datasource);

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
