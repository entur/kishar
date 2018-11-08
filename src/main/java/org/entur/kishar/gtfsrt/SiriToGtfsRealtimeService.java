/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */
package org.entur.kishar.gtfsrt;

import com.google.common.base.Preconditions;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import org.entur.kishar.gtfsrt.siri.SiriLibrary;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeLibrary;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeMutableProvider;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProvider;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProviderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.*;
import uk.org.siri.siri20.SituationExchangeDeliveryStructure.Situations;
import uk.org.siri.siri20.VehicleActivityStructure.MonitoredVehicleJourney;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

@Service
public class SiriToGtfsRealtimeService {

    public static final String MONITORING_ERROR_NO_CURRENT_INFORMATION = "NO_CURRENT_INFORMATION";

    public static final String MONITORING_ERROR_NOMINALLY_LOCATED = "NOMINALLY_LOCATED";

    private static Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

    private AlertFactory alertFactory;

    private Map<TripAndVehicleKey, VehicleData> dataByVehicle = new HashMap<>();

    private Map<TripAndVehicleKey, EstimatedVehicleJourneyData> dataByTimetable = new HashMap<>();

    private Map<String, AlertData> alertDataById = new HashMap<>();

    private boolean newData = false;

    /**
     * Time, in seconds, after which a vehicle update is considered stale
     */
    private static final int staleDataThreshold = 5 * 60;

    public SiriToGtfsRealtimeService(@Autowired AlertFactory alertFactory) {
        this.alertFactory = alertFactory;
        this.gtfsRealtimeProvider = new GtfsRealtimeProviderImpl();
    }

    private Set<String> _monitoringErrorsForVehiclePositions = new HashSet<String>() {
        private static final long serialVersionUID = 1L;

        {
            add(MONITORING_ERROR_NO_CURRENT_INFORMATION);
            add(MONITORING_ERROR_NOMINALLY_LOCATED);
        }
    };

    private GtfsRealtimeMutableProvider gtfsRealtimeProvider;

    public Object getTripUpdates(String contentType) {
        return encodeFeedMessage(gtfsRealtimeProvider.getTripUpdates(), contentType);
    }
    public Object getVehiclePositions(String contentType) {
        return encodeFeedMessage(gtfsRealtimeProvider.getVehiclePositions(), contentType);
    }
    public Object getAlerts(String contentType) {
        return encodeFeedMessage(gtfsRealtimeProvider.getAlerts(), contentType);
    }

    private Object encodeFeedMessage(FeedMessage feedMessage, String contentType) {
        if (contentType != null && contentType.equals(MediaType.APPLICATION_JSON)) {
            return feedMessage;
        }
        return feedMessage.toByteArray();
    }

    public void processDelivery(Siri siri) throws IOException {
        newData = true;
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();

        for (EstimatedTimetableDeliveryStructure etDelivery : serviceDelivery.getEstimatedTimetableDeliveries()) {
            List<EstimatedVersionFrameStructure> estimatedJourneyVersionFrames = etDelivery.getEstimatedJourneyVersionFrames();
            if (estimatedJourneyVersionFrames == null) {
                continue;
            }
            for (EstimatedVersionFrameStructure estimatedJourneyVersionFrame : estimatedJourneyVersionFrames) {
                List<EstimatedVehicleJourney> estimatedVehicleJourneies = estimatedJourneyVersionFrame.getEstimatedVehicleJourneies();
                long t1 = System.currentTimeMillis();
                Map<String, Integer> errorMap = new HashMap<>();
                for (EstimatedVehicleJourney estimatedVehicleJourney : estimatedVehicleJourneies) {
                    try {
                        process(serviceDelivery, estimatedVehicleJourney);
                    } catch (NullPointerException ex) {
                        LOG.debug("EstimatedVehicleJourney with missing data: {}.", ex.getMessage());
                        String key = "Missing " + ex.getMessage();
                        errorMap.put(key, errorMap.getOrDefault(key, 0) + 1);
                        continue;
                    }
                }
                LOG.info("Processed {} estimatedVehicleJourneys in {} ms", estimatedVehicleJourneies.size(), (System.currentTimeMillis()-t1));
                logErrorMap( "Errors ET", errorMap);
            }
        }

        for (VehicleMonitoringDeliveryStructure vmDelivery : serviceDelivery.getVehicleMonitoringDeliveries()) {
            long t1 = System.currentTimeMillis();
            Map<String, Integer> errorMap = new HashMap<>();
            for (VehicleActivityStructure vehicleActivity : vmDelivery.getVehicleActivities()) {

                try {
                    process(serviceDelivery, vehicleActivity);
                } catch (NullPointerException ex) {
                    LOG.debug("VehicleActivity with missing: {}.", ex.getMessage());
                    String key = "Missing " + ex.getMessage();
                    errorMap.put(key, errorMap.getOrDefault(key, 0) + 1);
                    continue;
                }
            }
            LOG.info("Processed {} VehicleActivities in {} ms", vmDelivery.getVehicleActivities().size(), (System.currentTimeMillis()-t1));
            logErrorMap( "Errors VM", errorMap);
        }
        for (SituationExchangeDeliveryStructure sxDelivery : serviceDelivery.getSituationExchangeDeliveries()) {
            Situations situations = sxDelivery.getSituations();
            if (situations == null) {
                continue;
            }
            long t1 = System.currentTimeMillis();
            for (PtSituationElement situation : situations.getPtSituationElements()) {
                process(serviceDelivery, situation);
            }
            LOG.info("Processed {} Situations in {} ms", situations.getPtSituationElements().size(), (System.currentTimeMillis()-t1));
        }
    }

    private void logErrorMap(String title, Map<String, Integer> errorMap) {
        if (!errorMap.isEmpty()) {
            StringBuilder builder = new StringBuilder(title + ": ");
            for (String s : errorMap.keySet()) {
                builder.append(s)
                        .append(": ")
                        .append(errorMap.get(s));
            }
            LOG.info(builder.toString());
        }
    }


    /****
     * Private Methods
     ****/

    private void process(ServiceDelivery delivery,
                         VehicleActivityStructure vehicleActivity) {
        checkForMissingElements(vehicleActivity);

        TripAndVehicleKey key = getKey(vehicleActivity);

        String producer = null;
        if (delivery.getProducerRef() != null) {
            producer = delivery.getProducerRef().getValue();
        }

        VehicleData data = new VehicleData(key, System.currentTimeMillis(),
                vehicleActivity, producer);
        dataByVehicle.put(key, data);
    }


    private void process(ServiceDelivery delivery,
                         EstimatedVehicleJourney estimatedVehicleJourney) {

        checkForMissingElements(estimatedVehicleJourney);

        TripAndVehicleKey key = getKey(estimatedVehicleJourney);

        String producer = null;
        if (delivery.getProducerRef() != null) {
            producer = delivery.getProducerRef().getValue();
        }

        EstimatedVehicleJourneyData data = new EstimatedVehicleJourneyData(key, System.currentTimeMillis(),
                estimatedVehicleJourney, producer);
        dataByTimetable.put(key, data);
    }

    private void checkForMissingElements(VehicleActivityStructure vehicleActivity) {

        Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        FramedVehicleJourneyRefStructure fvjRef = vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef();

        Preconditions.checkNotNull(fvjRef,"FramedVehicleJourneyRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef().getValue(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDatedVehicleJourneyRef(),"DatedVehicleJourneyRef");

    }

    private void checkForMissingElements(EstimatedVehicleJourney estimatedVehicleJourney) {

        FramedVehicleJourneyRefStructure fvjRef = estimatedVehicleJourney.getFramedVehicleJourneyRef();

        Preconditions.checkNotNull(fvjRef,"FramedVehicleJourneyRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef().getValue(),"DataFrameRef");

        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls(), "EstimatedCalls");
        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls(), "EstimatedCalls");

    }

    private TripAndVehicleKey getKey(VehicleActivityStructure vehicleActivity) {

        MonitoredVehicleJourney mvj = vehicleActivity.getMonitoredVehicleJourney();

        return getTripAndVehicleKey(mvj.getVehicleRef(), mvj.getFramedVehicleJourneyRef());
    }

    private TripAndVehicleKey getKey(EstimatedVehicleJourney vehicleJourney) {
        return getTripAndVehicleKey(vehicleJourney.getVehicleRef(), vehicleJourney.getFramedVehicleJourneyRef());
    }

    private TripAndVehicleKey getTripAndVehicleKey(VehicleRef vehicleRef, FramedVehicleJourneyRefStructure fvjRef) {
        String vehicle = null;
        if (vehicleRef != null && vehicleRef.getValue() != null) {
            vehicle = vehicleRef.getValue();
        }

        return TripAndVehicleKey.fromTripIdServiceDateAndVehicleId(
                fvjRef.getDatedVehicleJourneyRef(), fvjRef.getDataFrameRef().getValue(), vehicle);
    }

    private void process(ServiceDelivery delivery, PtSituationElement situation) {
        SituationNumber situationNumber = situation.getSituationNumber();
        if (situationNumber == null || situationNumber.getValue() == null) {
            LOG.warn("PtSituationElement did not specify a SituationNumber");
        }
        String id = situationNumber.getValue();
        String producer = null;
        if (delivery.getProducerRef() != null) {
            producer = delivery.getProducerRef().getValue();
        }
        AlertData data = new AlertData(situation, producer, null);
        alertDataById.put(id, data);
    }

    public void writeOutput() throws IOException {
        if (!newData) {
            LOG.info("No new data received - ignore updating output.");
            return;
        }
        newData = false;
        long t1 = System.currentTimeMillis();
        writeTripUpdates();
        writeVehiclePositions();
        writeAlerts();
        gtfsRealtimeProvider.fireUpdate();
        LOG.info("Wrote output in {} ms: {} alerts, {} vehicle-positions, {} trip-updates",
                (System.currentTimeMillis()-t1),
                gtfsRealtimeProvider.getAlerts().getEntityCount(),
                gtfsRealtimeProvider.getVehiclePositions().getEntityCount(),
                gtfsRealtimeProvider.getTripUpdates().getEntityCount());
    }

    private void writeTripUpdates() throws IOException {

        FeedMessage.Builder feedMessageBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();
        long feedTimestamp = feedMessageBuilder.getHeader().getTimestamp() * 1000;

        for (Iterator<EstimatedVehicleJourneyData> it = dataByTimetable.values().iterator(); it.hasNext(); ) {
            EstimatedVehicleJourneyData data = it.next();

            if (isDataStale(data.getTimestamp(), feedTimestamp)) {
                it.remove();
                continue;
            }

            EstimatedVehicleJourney vehicleJourney = data.geEstimatedVehicleJourney();

            if (vehicleJourney.isMonitored() != null && vehicleJourney.isMonitored()) {
                if (vehicleJourney.isCancellation() != null && !vehicleJourney.isCancellation()) {
                    continue;
                }
            }

            TripUpdate.Builder tripUpdate = TripUpdate.newBuilder();

            TripDescriptor td = getEstimatedVehicleJourneyAsTripDescriptor(vehicleJourney);
            tripUpdate.setTrip(td);

            VehicleDescriptor vd = getEstimatedVehicleJourneyAsVehicleDescriptor(vehicleJourney);
            if (vd != null) {
                tripUpdate.setVehicle(vd);
            }

            applyStopSpecificDelayToTripUpdateIfApplicable(vehicleJourney, tripUpdate);


            FeedEntity.Builder entity = FeedEntity.newBuilder();
            entity.setId(getTripIdForEstimatedVehicleJourney(vehicleJourney));

            entity.setTripUpdate(tripUpdate);
            feedMessageBuilder.addEntity(entity);
        }

        gtfsRealtimeProvider.setTripUpdates(feedMessageBuilder.build(), false);
    }

    private void applyStopSpecificDelayToTripUpdateIfApplicable(
            EstimatedVehicleJourney mvj,
            TripUpdate.Builder tripUpdate) {
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = mvj.getEstimatedCalls();
        EstimatedVehicleJourney.RecordedCalls recordedCalls = mvj.getRecordedCalls();

        int stopCounter = 0;
        if (recordedCalls != null && recordedCalls.getRecordedCalls() != null) {
            for (RecordedCall recordedCall : recordedCalls.getRecordedCalls()) {
                StopPointRef stopPointRef = recordedCall.getStopPointRef();
                if (stopPointRef == null || stopPointRef.getValue() == null) {
                    return;
                }
                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (recordedCall.getAimedArrivalTime() != null) {
                    arrivalDelayInSeconds = calculateDiff(recordedCall.getAimedArrivalTime(), recordedCall.getActualArrivalTime());
                }

                if (recordedCall.getAimedDepartureTime() != null) {
                    departureDelayInSeconds = calculateDiff(recordedCall.getAimedDepartureTime(), recordedCall.getActualDepartureTime());
                }

                int stopSequence;
                if (recordedCall.getOrder() != null) {
                    stopSequence = recordedCall.getOrder().intValue() - 1;
                } else {
                    stopSequence = stopCounter;
                }

                addStopTimeUpdate(stopPointRef, arrivalDelayInSeconds, departureDelayInSeconds, stopSequence, tripUpdate);

                stopCounter++;
            }
        }
        if (estimatedCalls != null && estimatedCalls.getEstimatedCalls() != null) {
            for (EstimatedCall estimatedCall : estimatedCalls.getEstimatedCalls()) {
                StopPointRef stopPointRef = estimatedCall.getStopPointRef();
                if (stopPointRef == null || stopPointRef.getValue() == null) {
                    return;
                }

                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (estimatedCall.getAimedArrivalTime() != null){
                    arrivalDelayInSeconds = calculateDiff(estimatedCall.getAimedArrivalTime(), estimatedCall.getExpectedArrivalTime());
                }
                if (estimatedCall.getAimedDepartureTime() != null) {
                    departureDelayInSeconds = calculateDiff(estimatedCall.getAimedDepartureTime(), estimatedCall.getExpectedDepartureTime());
                }


                int stopSequence;
                if (estimatedCall.getOrder() != null) {
                    stopSequence = estimatedCall.getOrder().intValue() - 1;
                } else {
                    stopSequence = stopCounter;
                }

                addStopTimeUpdate(stopPointRef, arrivalDelayInSeconds, departureDelayInSeconds, stopSequence, tripUpdate);

                stopCounter++;
            }
        }
    }

    private void addStopTimeUpdate(StopPointRef stopPointRef, Integer arrivalDelayInSeconds, Integer departureDelayInSeconds, int stopSequence, TripUpdate.Builder tripUpdate) {


        StopTimeUpdate.Builder stopTimeUpdate = StopTimeUpdate.newBuilder();

        if (arrivalDelayInSeconds != null) {
            StopTimeEvent.Builder arrivalStopTimeEvent = StopTimeEvent.newBuilder();
            arrivalStopTimeEvent.setDelay(arrivalDelayInSeconds);
            stopTimeUpdate.setArrival(arrivalStopTimeEvent);
        }
        if (departureDelayInSeconds != null) {
            StopTimeEvent.Builder departureStopTimeEvent = StopTimeEvent.newBuilder();
            departureStopTimeEvent.setDelay(departureDelayInSeconds);
            stopTimeUpdate.setDeparture(departureStopTimeEvent);
        }

        stopTimeUpdate.setStopSequence(stopSequence);
        stopTimeUpdate.setStopId(stopPointRef.getValue());

        tripUpdate.addStopTimeUpdate(stopTimeUpdate);
    }

    private Integer calculateDiff(ZonedDateTime aimed, ZonedDateTime expected) {
        if (aimed != null && expected != null) {
            return (int)(expected.toEpochSecond() - aimed.toEpochSecond());
        }
        return null;
    }
    private String getTripIdForEstimatedVehicleJourney(EstimatedVehicleJourney mvj) {
        StringBuilder b = new StringBuilder();
        FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
        b.append((fvjRef.getDatedVehicleJourneyRef()));
        b.append('-');
        b.append(fvjRef.getDataFrameRef().getValue());
        if (mvj.getVehicleRef() != null && mvj.getVehicleRef().getValue() != null) {
            b.append('-');
            b.append(mvj.getVehicleRef().getValue());
        }
        return b.toString();
    }

    private void writeVehiclePositions() throws IOException {

        FeedMessage.Builder feedMessageBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();
        long feedTimestamp = feedMessageBuilder.getHeader().getTimestamp() * 1000;

        for (Iterator<VehicleData> it = dataByVehicle.values().iterator(); it.hasNext(); ) {
            VehicleData data = it.next();
            if (isDataStale(data.getTimestamp(), feedTimestamp)) {
                it.remove();
                continue;
            }

            TripAndVehicleKey key = data.getKey();
            VehicleActivityStructure activity = data.getVehicleActivity();

            MonitoredVehicleJourney mvj = activity.getMonitoredVehicleJourney();
            LocationStructure location = mvj.getVehicleLocation();

            if (hasVehiclePositionMonitoringError(mvj)) {
                continue;
            }

            if (location != null && location.getLatitude() != null
                    && location.getLongitude() != null) {

                VehiclePosition.Builder vp = VehiclePosition.newBuilder();

                TripDescriptor td = getMonitoredVehicleJourneyAsTripDescriptor(mvj);
                vp.setTrip(td);

                VehicleDescriptor vd = getMonitoredVehicleJourneyAsVehicleDescriptor(mvj);
                if (vd != null) {
                    vp.setVehicle(vd);
                }

                Instant time = activity.getRecordedAtTime().toInstant();
                if (time == null) {
                    time = Instant.ofEpochMilli(feedTimestamp);
                }
                vp.setTimestamp(time.getEpochSecond());

                Position.Builder position = Position.newBuilder();
                position.setLatitude(location.getLatitude().floatValue());
                position.setLongitude(location.getLongitude().floatValue());
                vp.setPosition(position);

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                entity.setId(getVehicleIdForKey(key));

                entity.setVehicle(vp);
                feedMessageBuilder.addEntity(entity);
            }
        }

        gtfsRealtimeProvider.setVehiclePositions(feedMessageBuilder.build(), false);
    }

    private String getVehicleIdForKey(TripAndVehicleKey key) {
        if (key.getVehicleId() != null) {
            return key.getVehicleId();
        }
        return key.getTripId() + "-"
                + key.getServiceDate();
    }

    private void writeAlerts() {
        FeedMessage.Builder feedMessageBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();

        ZonedDateTime now = ZonedDateTime.now();

        for (Iterator<AlertData> it = alertDataById.values().iterator(); it.hasNext(); ) {
            AlertData data = it.next();

            PtSituationElement situation = data.getSituation();

            /**
             * If the situation has been closed or has expired, we no longer show the
             * alert in the GTFS-realtime feed.
             */
            if (SiriLibrary.isSituationClosed(situation)
                    || SiriLibrary.isSituationExpired(situation, now)
                    || SiriLibrary.isAlertDataExpired(data)) {
                it.remove();
                continue;
            }

            /**
             * If the situation is not in a valid period, we no longer show the alert
             * in the GTFS-realtime feed.
             */
            if (!SiriLibrary.isSituationPublishedOrValid(situation, now)) {
                continue;
            }

            Alert.Builder alert = alertFactory.createAlertFromSituation(situation);

            FeedEntity.Builder entity = FeedEntity.newBuilder();
            entity.setId(situation.getSituationNumber().getValue());

            entity.setAlert(alert);
            feedMessageBuilder.addEntity(entity);
        }

        gtfsRealtimeProvider.setAlerts(feedMessageBuilder.build(), false);
    }



    private boolean isDataStale(long timestamp, long currentTime) {
        return timestamp + staleDataThreshold * 1000 < currentTime;
    }

    /**
     *
     * @param mvj
     * @return true if MonitoredVehicleJourney.Monitored is false and
     *         MonitoredVehicleJourney.MonitoringError contains a string matching
     *         a value in {@link #_monitoringErrorsForVehiclePositions}.
     */
    private boolean hasVehiclePositionMonitoringError(MonitoredVehicleJourney mvj) {
        if (mvj.isMonitored() != null && mvj.isMonitored()) {
            return false;
        }
        List<String> errors = mvj.getMonitoringError();
        if (errors == null) {
            return false;
        }
        for (String error : errors) {
            if (_monitoringErrorsForVehiclePositions.contains(error)) {
                return true;
            }
        }
        return false;
    }

    private TripDescriptor getEstimatedVehicleJourneyAsTripDescriptor(EstimatedVehicleJourney estimatedVehicleJourney) {

        TripDescriptor.Builder td = TripDescriptor.newBuilder();
        FramedVehicleJourneyRefStructure fvjRef = estimatedVehicleJourney.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (estimatedVehicleJourney.getLineRef() != null) {
            td.setRouteId(estimatedVehicleJourney.getLineRef().getValue());
        }

        return td.build();
    }

    private TripDescriptor getMonitoredVehicleJourneyAsTripDescriptor(MonitoredVehicleJourney mvj) {

        TripDescriptor.Builder td = TripDescriptor.newBuilder();
        FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (mvj.getLineRef() != null) {
            td.setRouteId(mvj.getLineRef().getValue());
        }

        return td.build();
    }

    private VehicleDescriptor getMonitoredVehicleJourneyAsVehicleDescriptor(MonitoredVehicleJourney mvj) {
        VehicleRef vehicleRef = mvj.getVehicleRef();
        if (vehicleRef == null || vehicleRef.getValue() == null) {
            return null;
        }

        VehicleDescriptor.Builder vd = VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

    private VehicleDescriptor getEstimatedVehicleJourneyAsVehicleDescriptor(
            EstimatedVehicleJourney estimatedVehicleJourney) {
        VehicleRef vehicleRef = estimatedVehicleJourney.getVehicleRef();

        if (vehicleRef == null || vehicleRef.getValue() == null) {
            return null;
        }

        VehicleDescriptor.Builder vd = VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

}
