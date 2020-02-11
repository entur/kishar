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
import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.*;
import uk.org.siri.siri20.SituationExchangeDeliveryStructure.Situations;
import uk.org.siri.siri20.VehicleActivityStructure.MonitoredVehicleJourney;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.createFeedMessageBuilder;

@Service
public class SiriToGtfsRealtimeService {
    private static Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

    public static final String MONITORING_ERROR_NO_CURRENT_INFORMATION = "NO_CURRENT_INFORMATION";

    public static final String MONITORING_ERROR_NOMINALLY_LOCATED = "NOMINALLY_LOCATED";

    private static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";

    private AlertFactory alertFactory;

    private Map<TripAndVehicleKey, VehicleData> dataByVehicle = new ConcurrentHashMap<>();

    private Map<TripAndVehicleKey, EstimatedVehicleJourneyData> dataByTimetable = new ConcurrentHashMap<>();

    private Map<String, AlertData> alertDataById = new ConcurrentHashMap<>();

    private boolean newData = false;

    private List<String> datasourceETWhitelist;

    private List<String> datasourceVMWhitelist;

    private List<String> datasourceSXWhitelist;

    @Autowired
    private PrometheusMetricsService prometheusMetricsService;

    /**
     * Time, in seconds, after which a vehicle update is considered stale
     */
    private static final int staleDataThreshold = 5 * 60;
    private FeedMessage tripUpdates = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> tripUpdatesByDatasource = Maps.newHashMap();
    private FeedMessage vehiclePositions = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> vehiclePositionsByDatasource = Maps.newHashMap();
    private FeedMessage alerts = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> alertsByDatasource = Maps.newHashMap();

    private int closeToNextStopPercentage;

    private int closeToNextStopDistance;

    public SiriToGtfsRealtimeService(@Autowired AlertFactory alertFactory,
                                     @Value("kishar.datasource.et.whitelist") List<String> datasourceETWhitelist,
                                     @Value("kishar.datasource.vm.whitelist") List<String> datasourceVMWhitelist,
                                     @Value("kishar.datasource.sx.whitelist") List<String> datasourceSXWhitelist,
                                     @Value("kishar.settings.vm.close.to.stop.percentage") int closeToNextStopPercentage,
                                     @Value("kishar.settings.vm.close.to.stop.distance") int closeToNextStopDistance) {
        this.datasourceETWhitelist = datasourceETWhitelist;
        this.datasourceVMWhitelist = datasourceVMWhitelist;
        this.datasourceSXWhitelist = datasourceSXWhitelist;
        this.alertFactory = alertFactory;
        this.closeToNextStopPercentage = closeToNextStopPercentage;
        this.closeToNextStopDistance = closeToNextStopDistance;
    }

    private Set<String> _monitoringErrorsForVehiclePositions = new HashSet<String>() {
        private static final long serialVersionUID = 1L;

        {
            add(MONITORING_ERROR_NO_CURRENT_INFORMATION);
            add(MONITORING_ERROR_NOMINALLY_LOCATED);
        }
    };

    public Object getTripUpdates(String contentType, String datasource) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_ET", 1);
        }
        FeedMessage feedMessage = tripUpdates;
        if (datasource != null && !datasource.isEmpty()) {
            feedMessage = tripUpdatesByDatasource.get(datasource);
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        }
        return encodeFeedMessage(feedMessage, contentType);
    }
    public Object getVehiclePositions(String contentType, String datasource) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_VM", 1);
        }
        FeedMessage feedMessage = vehiclePositions;
        if (datasource != null && !datasource.isEmpty()) {
            feedMessage = vehiclePositionsByDatasource.get(datasource);
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        }
        return encodeFeedMessage(feedMessage, contentType);
    }
    public Object getAlerts(String contentType, String datasource) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_SX", 1);
        }
        FeedMessage feedMessage = alerts;
        if (datasource != null && !datasource.isEmpty()) {
            feedMessage = alertsByDatasource.get(datasource);
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        }
        return encodeFeedMessage(feedMessage, contentType);
    }

    private Object encodeFeedMessage(FeedMessage feedMessage, String contentType) {

        if (contentType != null && contentType.equals(MEDIA_TYPE_APPLICATION_JSON)) {
            return feedMessage;
        }
        if (feedMessage != null) {
            return feedMessage.toByteArray();
        }
        return null;
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
                    } catch (IllegalStateException ex) {
                        LOG.debug("EstimatedVehicleJourney with precondition failed: {}.", ex.getMessage());
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
                } catch (IllegalStateException ex) {
                    LOG.debug("VehicleActivity with precondition failed: {}.", ex.getMessage());
                    continue;
                }
            }
            LOG.info("Processed {} VehicleActivities in {} ms", vmDelivery.getVehicleActivities().size(), (System.currentTimeMillis()-t1));
            logErrorMap( "Errors VM", errorMap);
        }
        for (SituationExchangeDeliveryStructure sxDelivery : serviceDelivery.getSituationExchangeDeliveries()) {
            Map<String, Integer> errorMap = new HashMap<>();
            Situations situations = sxDelivery.getSituations();
            if (situations == null) {
                continue;
            }
            long t1 = System.currentTimeMillis();
            for (PtSituationElement situation : situations.getPtSituationElements()) {
                try {
                    process(serviceDelivery, situation);
                } catch (NullPointerException ex) {
                    LOG.debug("Situation with missing: {}.", ex.getMessage());
                    String key = "Missing " + ex.getMessage();
                    errorMap.put(key, errorMap.getOrDefault(key, 0) + 1);
                    continue;
                } catch (IllegalStateException ex) {
                    LOG.debug("Situation with precondition failed: {}.", ex.getMessage());
                    continue;
                }
            }
            LOG.info("Processed {} Situations in {} ms", situations.getPtSituationElements().size(), (System.currentTimeMillis()-t1));
            logErrorMap( "Errors VM", errorMap);
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
        checkPreconditions(vehicleActivity);

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

        checkPreconditions(estimatedVehicleJourney);

        TripAndVehicleKey key = getKey(estimatedVehicleJourney);

        String producer = null;
        if (delivery.getProducerRef() != null) {
            producer = delivery.getProducerRef().getValue();
        }

        EstimatedVehicleJourneyData data = new EstimatedVehicleJourneyData(key, System.currentTimeMillis(),
                estimatedVehicleJourney, producer);
        dataByTimetable.put(key, data);
    }

    private void checkPreconditions(VehicleActivityStructure vehicleActivity) {

        Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        String datasource = vehicleActivity.getMonitoredVehicleJourney().getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");

        if (datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty()) {
            if (prometheusMetricsService != null) {
                if (datasourceVMWhitelist.contains(datasource)) {
                    prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, true);
                } else {
                    prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, false);
                }
            }
            Preconditions.checkState(datasourceVMWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }

        checkPreconditions(vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef());

    }

    private void checkPreconditions(EstimatedVehicleJourney estimatedVehicleJourney) {

        checkPreconditions(estimatedVehicleJourney.getFramedVehicleJourneyRef());

        String datasource = estimatedVehicleJourney.getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");

        if (datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty()) {
            if (prometheusMetricsService != null) {
                if (datasourceETWhitelist.contains(datasource)) {
                    prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, true);
                } else {
                    prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, false);
                }
            }
            Preconditions.checkState(datasourceETWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }
        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls(), "EstimatedCalls");
        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls().getEstimatedCalls(), "EstimatedCalls");

    }

    private void checkPreconditions(PtSituationElement situation) {
        Preconditions.checkNotNull(situation.getSituationNumber());
        Preconditions.checkNotNull(situation.getSituationNumber().getValue());

        Preconditions.checkNotNull(situation.getParticipantRef(), "datasource");
        String datasource = situation.getParticipantRef().getValue();
        Preconditions.checkNotNull(datasource, "datasource");

        if (datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty()) {
            if (prometheusMetricsService != null) {
                if (datasourceSXWhitelist.contains(datasource)) {
                    prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, true);
                } else {
                    prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, false);
                }
            }
            Preconditions.checkState(datasourceSXWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }
    }

    private void checkPreconditions(FramedVehicleJourneyRefStructure fvjRef) {
        Preconditions.checkNotNull(fvjRef,"FramedVehicleJourneyRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef().getValue(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDatedVehicleJourneyRef(),"DatedVehicleJourneyRef");
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
        checkPreconditions(situation);
        String id = situation.getSituationNumber().getValue();
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
        LOG.info("Wrote output in {} ms: {} alerts, {} vehicle-positions, {} trip-updates",
                (System.currentTimeMillis()-t1),
                alerts.getEntityCount(),
                vehiclePositions.getEntityCount(),
                tripUpdates.getEntityCount());
    }

    private void writeTripUpdates() throws IOException {

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();
        long feedTimestamp = feedMessageBuilder.getHeader().getTimestamp() * 1000;

        for (Iterator<EstimatedVehicleJourneyData> it = dataByTimetable.values().iterator(); it.hasNext(); ) {
            EstimatedVehicleJourneyData data = it.next();

            if (isDataStale(data.getTimestamp(), feedTimestamp)) {
                it.remove();
                continue;
            }

            EstimatedVehicleJourney vehicleJourney = data.getEstimatedVehicleJourney();

            if (vehicleJourney.isMonitored() != null && vehicleJourney.isMonitored()) {
                if (vehicleJourney.isCancellation() != null && !vehicleJourney.isCancellation()) {
                    continue;
                }
            }

            String datasource = vehicleJourney.getDataSource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
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
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }

        setTripUpdates(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
    }

    private Map<String, FeedMessage> buildFeedMessageMap(Map<String, FeedMessage.Builder> feedMessageBuilderMap) {
        Map<String, FeedMessage> feedMessageMap = Maps.newHashMap();
        for (String key : feedMessageBuilderMap.keySet()) {
            feedMessageMap.put(key, feedMessageBuilderMap.get(key).build());
        }
        return feedMessageMap;
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

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();
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

            if (hasVehiclePositionMonitoringError(mvj)) {
                continue;
            }

            String datasource = mvj.getDataSource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            VehiclePosition.Builder vp = convertSiriToGtfsRt(activity);

            if (vp != null) {

                if (vp.getTimestamp() <= 0) {
                    vp.setTimestamp(feedTimestamp);
                }

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                entity.setId(getVehicleIdForKey(key));

                entity.setVehicle(vp);
                feedMessageBuilder.addEntity(entity);
                feedMessageBuilderByDatasource.addEntity(entity);
                feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
            }
        }

        setVehiclePositions(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
    }

    private VehiclePosition.Builder convertSiriToGtfsRt(VehicleActivityStructure activity) {
        if (activity == null) {
            return null;
        }

        MonitoredVehicleJourney mvj = activity.getMonitoredVehicleJourney();
        if (mvj == null) {
            return null;
        }

        LocationStructure location = mvj.getVehicleLocation();
        VehiclePosition.Builder vp = VehiclePosition.newBuilder();;

        if (location != null && location.getLatitude() != null
                && location.getLongitude() != null) {

            vp = VehiclePosition.newBuilder();

            TripDescriptor td = getMonitoredVehicleJourneyAsTripDescriptor(mvj);
            vp.setTrip(td);

            VehicleDescriptor vd = getMonitoredVehicleJourneyAsVehicleDescriptor(mvj);
            if (vd != null) {
                vp.setVehicle(vd);
            }

            Instant time = activity.getRecordedAtTime().toInstant();
            if (time != null) {
                vp.setTimestamp(time.getEpochSecond());
            }

            Position.Builder position = Position.newBuilder();
            position.setLatitude(location.getLatitude().floatValue());
            position.setLongitude(location.getLongitude().floatValue());

            if ( mvj.getBearing() != null) {
                position.setBearing(mvj.getBearing().floatValue());
            }

            // Speed - not included in profile
            if ( mvj.getVelocity() != null) {
                position.setSpeed(mvj.getVelocity().floatValue());
            }

            //Distance traveled since last stop
            boolean isCloseToNextStop = false;
            if (activity.getProgressBetweenStops() != null) {
                final ProgressBetweenStopsStructure progressBetweenStops = activity.getProgressBetweenStops();
                if (progressBetweenStops.getPercentage() != null) {

                    isCloseToNextStop = progressBetweenStops.getPercentage().intValue() > closeToNextStopPercentage;

                    final BigDecimal linkDistance = progressBetweenStops.getLinkDistance();

                    if (linkDistance != null) {
                        final BigDecimal distanceTravelled = linkDistance.multiply(progressBetweenStops.getPercentage().divide(BigDecimal.valueOf(100)));
                        position.setOdometer(distanceTravelled.doubleValue());

                        if (linkDistance.doubleValue() - distanceTravelled.doubleValue() < closeToNextStopDistance) {
                            isCloseToNextStop = true;
                        }
                    }
                }
            }


            // VehicleStatus
            if (mvj.getMonitoredCall() != null) {
                final MonitoredCallStructure monitoredCall = mvj.getMonitoredCall();
                if (monitoredCall.isVehicleAtStop() != null && monitoredCall.isVehicleAtStop()) {
                    vp.setCurrentStatus(VehiclePosition.VehicleStopStatus.STOPPED_AT);
                } else if (isCloseToNextStop) {
                    vp.setCurrentStatus(VehiclePosition.VehicleStopStatus.INCOMING_AT);
                } else {
                    vp.setCurrentStatus(VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO);
                }

                final LocationStructure locationAtStop = monitoredCall.getVehicleLocationAtStop();
                if (locationAtStop != null && locationAtStop.getLatitude() != null && locationAtStop.getLongitude() != null) {
                    position.setLatitude(locationAtStop.getLatitude().floatValue());
                    position.setLongitude(locationAtStop.getLongitude().floatValue());
                }

                if (monitoredCall.getStopPointRef() != null) {
                    vp.setStopId(monitoredCall.getStopPointRef().getValue());
                }

                if (monitoredCall.getOrder() != null) {
                    //TODO: Correct to assume that order == stop_sequence?
                    vp.setCurrentStopSequence(monitoredCall.getOrder().intValue());
                }
            }

            vp.setPosition(position);

            //Occupancy - GTFS-RT experimental feature
            if (mvj.getOccupancy() != null) {
                switch (mvj.getOccupancy()) {
                    case FULL:
                        vp.setOccupancyStatus(VehiclePosition.OccupancyStatus.FULL);
                        break;
                    case STANDING_AVAILABLE:
                        vp.setOccupancyStatus(VehiclePosition.OccupancyStatus.STANDING_ROOM_ONLY);
                        break;
                    case SEATS_AVAILABLE:
                        vp.setOccupancyStatus(VehiclePosition.OccupancyStatus.FEW_SEATS_AVAILABLE);
                        break;
                }
            }

            //Congestion
            if (mvj.isInCongestion() != null) {
                if (mvj.isInCongestion()) {
                    vp.setCongestionLevel(VehiclePosition.CongestionLevel.CONGESTION);
                } else {
                    vp.setCongestionLevel(VehiclePosition.CongestionLevel.RUNNING_SMOOTHLY);
                }
            }
        }
        return vp;
    }

    private String getVehicleIdForKey(TripAndVehicleKey key) {
        if (key.getVehicleId() != null) {
            return key.getVehicleId();
        }
        return key.getTripId() + "-"
                + key.getServiceDate();
    }

    private void writeAlerts() {
        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        ZonedDateTime now = ZonedDateTime.now();

        for (Iterator<AlertData> it = alertDataById.values().iterator(); it.hasNext(); ) {
            AlertData data = it.next();

            PtSituationElement situation = data.getSituation();

            String datasource = situation.getParticipantRef().getValue();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

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
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }

        setAlerts(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
    }

    public FeedMessage getTripUpdates() {
        return tripUpdates;
    }

    public void setTripUpdates(FeedMessage tripUpdates, Map<String, FeedMessage> tripUpdatesByDatasource) {
        this.tripUpdates = tripUpdates;
        this.tripUpdatesByDatasource = tripUpdatesByDatasource;
    }

    public FeedMessage getVehiclePositions() {
        return vehiclePositions;
    }

    public void setVehiclePositions(FeedMessage vehiclePositions, Map<String, FeedMessage> vehiclePositionsByDatasource) {
        this.vehiclePositions = vehiclePositions;
        this.vehiclePositionsByDatasource = vehiclePositionsByDatasource;
    }

    public FeedMessage getAlerts() {
        return alerts;
    }

    public void setAlerts(FeedMessage alerts, Map<String, FeedMessage> alertsByDatasource) {
        this.alerts = alerts;
        this.alertsByDatasource = alertsByDatasource;
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
