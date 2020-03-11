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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

    Map<TripAndVehicleKey, VehicleData> dataByVehicle = new ConcurrentHashMap<>();

    Map<TripAndVehicleKey, EstimatedVehicleJourneyData> dataByTimetable = new ConcurrentHashMap<>();

    Map<String, AlertData> alertDataById = new ConcurrentHashMap<>();

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

    private List<VehiclePosition.Builder> vehiclePositionBuilders = Lists.newArrayList();
    private Map<String, List<VehiclePosition.Builder>> vehiclePositionBuildersByDatasource = Maps.newHashMap();
    private List<TripUpdate.Builder> tripUpdatesBuilders = Lists.newArrayList();
    private Map<String, List<TripUpdate.Builder>> tripUpdatesBuildersByDatasource = Maps.newHashMap();
    private List<Alert.Builder> alertsBuilders = Lists.newArrayList();
    private Map<String, List<Alert.Builder>> alertsBuildersByDatasource = Maps.newHashMap();

    private GtfsRtMapper gtfsMapper;

    public SiriToGtfsRealtimeService(@Autowired AlertFactory alertFactory,
                                     @Value("${kishar.datasource.et.whitelist}") List<String> datasourceETWhitelist,
                                     @Value("${kishar.datasource.vm.whitelist}") List<String> datasourceVMWhitelist,
                                     @Value("${kishar.datasource.sx.whitelist}") List<String> datasourceSXWhitelist,
                                     @Value("${kishar.settings.vm.close.to.stop.percentage}") int closeToNextStopPercentage,
                                     @Value("${kishar.settings.vm.close.to.stop.distance}") int closeToNextStopDistance) {
        this.datasourceETWhitelist = datasourceETWhitelist;
        this.datasourceVMWhitelist = datasourceVMWhitelist;
        this.datasourceSXWhitelist = datasourceSXWhitelist;
        this.alertFactory = alertFactory;
        this.gtfsMapper = new GtfsRtMapper(closeToNextStopPercentage, closeToNextStopDistance);
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

    public void processDelivery(Siri siri) {
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
        checkPreconditions(vehicleActivity, true);
    }

    private void checkPreconditions(VehicleActivityStructure vehicleActivity, boolean countMetric) {

        Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        String datasource = vehicleActivity.getMonitoredVehicleJourney().getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");

        if (countMetric && prometheusMetricsService != null) {
            if (datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty() && !datasourceVMWhitelist.contains(datasource)) {
                prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, true);
            } else {
                prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, false);
            }
        }

        if (datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty()) {
            Preconditions.checkState(datasourceVMWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }

        Preconditions.checkNotNull(vehicleActivity.getValidUntilTime(), "ValidUntilTime");
        Preconditions.checkState(vehicleActivity.getValidUntilTime().isAfter(ZonedDateTime.now()), "Message is no longer valid");

        checkPreconditions(vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef());

    }

    private void checkPreconditions(EstimatedVehicleJourney estimatedVehicleJourney) {

        checkPreconditions(estimatedVehicleJourney.getFramedVehicleJourneyRef());

        String datasource = estimatedVehicleJourney.getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");
        if (prometheusMetricsService != null) {
            if (datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty() && !datasourceETWhitelist.contains(datasource)) {
                prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, true);
            } else {
                prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, false);
            }
        }

        if (datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty()) {
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
        if (prometheusMetricsService != null) {
            if (datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty() && !datasourceSXWhitelist.contains(datasource)) {
                prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, true);
            } else {
                prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, false);
            }
        }

        if (datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty()) {
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

            TripUpdate.Builder tripUpdate = gtfsMapper.mapTripUpdateFromVehicleJourney(vehicleJourney);

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

            VehiclePosition.Builder vp = null;
            try {
                checkPreconditions(activity, false);

                vp = gtfsMapper.convertSiriToGtfsRt(activity);
            } catch (Throwable t) {
                LOG.info("Ignoring update, preconditions failed: " + t.getMessage());
            }

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

    public String getDatasourceFromSiriVm(Siri siri) {

        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getVehicleMonitoringDeliveries().size() > 0) {
                for (VehicleMonitoringDeliveryStructure deliveryStructure : serviceDelivery.getVehicleMonitoringDeliveries()) {
                    if (deliveryStructure != null && deliveryStructure.getVehicleActivities() != null) {
                        if (deliveryStructure.getVehicleActivities().size() > 0) {
                            for (VehicleActivityStructure activity : deliveryStructure.getVehicleActivities()) {
                                if (activity.getMonitoredVehicleJourney() != null && activity.getMonitoredVehicleJourney().getDataSource() != null) {
                                    return activity.getMonitoredVehicleJourney().getDataSource();
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public List<VehiclePosition.Builder> convertSiriVmToGtfsRt(Siri siri) {

        List<VehiclePosition.Builder> result = Lists.newArrayList();
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getVehicleMonitoringDeliveries().size() > 0) {
                for (VehicleMonitoringDeliveryStructure deliveryStructure : serviceDelivery.getVehicleMonitoringDeliveries()) {
                    if (deliveryStructure != null && deliveryStructure.getVehicleActivities() != null) {
                        if (deliveryStructure.getVehicleActivities().size() > 0) {
                            for (VehicleActivityStructure activity : deliveryStructure.getVehicleActivities()) {
                                result.add(gtfsMapper.convertSiriToGtfsRt(activity));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public void registerGtfsRtVehiclePosition(VehiclePosition.Builder vehiclePositions, String datasource) {
        vehiclePositionBuilders.add(vehiclePositions);
        if (vehiclePositionBuildersByDatasource.containsKey(datasource)) {
            vehiclePositionBuildersByDatasource.get(datasource).add(vehiclePositions);
        } else {
            List<VehiclePosition.Builder> list = Lists.newArrayList();
            list.add(vehiclePositions);
            vehiclePositionBuildersByDatasource.put(datasource, list);
        }
    }

    public String getDatasourceFromSiriEt(Siri siri) {
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getEstimatedTimetableDeliveries().size() > 0) {
                for (EstimatedTimetableDeliveryStructure estimatedTimetableDeliveryStructure : serviceDelivery.getEstimatedTimetableDeliveries()) {
                    if (estimatedTimetableDeliveryStructure != null && estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrames().size() > 0) {
                        for (EstimatedVersionFrameStructure estimatedVersionFrameStructure : estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrames()) {
                            if (estimatedVersionFrameStructure != null && estimatedVersionFrameStructure.getEstimatedVehicleJourneies().size() > 0) {
                                for (EstimatedVehicleJourney estimatedVehicleJourney : estimatedVersionFrameStructure.getEstimatedVehicleJourneies()) {
                                    if (estimatedVehicleJourney != null && estimatedVehicleJourney.getDataSource() != null) {
                                        return estimatedVehicleJourney.getDataSource();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public List<TripUpdate.Builder> convertSiriEtToGtfsRt(Siri siri) {

        List<TripUpdate.Builder> result = Lists.newArrayList();
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getEstimatedTimetableDeliveries().size() > 0) {
                for (EstimatedTimetableDeliveryStructure estimatedTimetableDeliveryStructure : serviceDelivery.getEstimatedTimetableDeliveries()) {
                    if (estimatedTimetableDeliveryStructure != null && estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrames().size() > 0) {
                        for (EstimatedVersionFrameStructure estimatedVersionFrameStructure : estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrames()) {
                            if (estimatedVersionFrameStructure != null && estimatedVersionFrameStructure.getEstimatedVehicleJourneies().size() > 0) {
                                for (EstimatedVehicleJourney estimatedVehicleJourney : estimatedVersionFrameStructure.getEstimatedVehicleJourneies()) {
                                    if (estimatedVehicleJourney != null) {
                                        result.add(gtfsMapper.mapTripUpdateFromVehicleJourney(estimatedVehicleJourney));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public void registerGtfsRtTripUpdate(TripUpdate.Builder tripUpdate, String datasource) {
        tripUpdatesBuilders.add(tripUpdate);
        if (tripUpdatesBuildersByDatasource.containsKey(datasource)) {
            tripUpdatesBuildersByDatasource.get(datasource).add(tripUpdate);
        } else {
            List<TripUpdate.Builder> list = Lists.newArrayList();
            list.add(tripUpdate);
            tripUpdatesBuildersByDatasource.put(datasource, list);
        }
    }

    public String getDatasourceFromSiriSx(Siri siri) {
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getSituationExchangeDeliveries().size() > 0) {
                for (SituationExchangeDeliveryStructure situationExchangeDeliveryStructure : serviceDelivery.getSituationExchangeDeliveries()) {
                    if (situationExchangeDeliveryStructure != null && situationExchangeDeliveryStructure.getSituations() != null && situationExchangeDeliveryStructure.getSituations().getPtSituationElements().size() > 0) {
                        for (PtSituationElement ptSituationElement : situationExchangeDeliveryStructure.getSituations().getPtSituationElements()) {
                            if (ptSituationElement != null && ptSituationElement.getParticipantRef() != null) {
                                return ptSituationElement.getParticipantRef().getValue();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public List<Alert.Builder> convertSiriSxToGtfsRt(Siri siri) {

        List<Alert.Builder> result = Lists.newArrayList();
        ServiceDelivery serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveries() != null) {
            if (serviceDelivery.getSituationExchangeDeliveries().size() > 0) {
                for (SituationExchangeDeliveryStructure situationExchangeDeliveryStructure : serviceDelivery.getSituationExchangeDeliveries()) {
                    if (situationExchangeDeliveryStructure != null && situationExchangeDeliveryStructure.getSituations() != null && situationExchangeDeliveryStructure.getSituations().getPtSituationElements().size() > 0) {
                        for (PtSituationElement ptSituationElement : situationExchangeDeliveryStructure.getSituations().getPtSituationElements()) {
                            if (ptSituationElement != null) {
                                result.add(alertFactory.createAlertFromSituation(ptSituationElement));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public void registerGtfsRtAlert(Alert.Builder alert, String datasource) {
        alertsBuilders.add(alert);
        if (alertsBuildersByDatasource.containsKey(datasource)) {
            alertsBuildersByDatasource.get(datasource).add(alert);
        } else {
            List<Alert.Builder> list = Lists.newArrayList();
            list.add(alert);
            alertsBuildersByDatasource.put(datasource, list);
        }
    }
}
