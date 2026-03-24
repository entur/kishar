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
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime.Alert;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;
import org.entur.avro.realtime.siri.model.EstimatedCallRecord;
import org.entur.avro.realtime.siri.model.EstimatedJourneyVersionFrameRecord;
import org.entur.avro.realtime.siri.model.EstimatedTimetableDeliveryRecord;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.FramedVehicleJourneyRefRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.RecordedCallRecord;
import org.entur.avro.realtime.siri.model.ServiceDeliveryRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.avro.realtime.siri.model.SituationExchangeDeliveryRecord;
import org.entur.avro.realtime.siri.model.ValidityPeriodRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.avro.realtime.siri.model.VehicleMonitoringDeliveryRecord;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.createFeedMessageBuilder;
import static org.entur.kishar.gtfsrt.mappers.AvroHelper.getInstant;

@Service
public class SiriToGtfsRealtimeService {
    private static final Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";

    private final AlertFactory alertFactory;

    private final List<String> datasourceETWhitelist;

    private final List<String> datasourceVMWhitelist;

    private final List<String> datasourceSXWhitelist;

    @Autowired
    private PrometheusMetricsService prometheusMetricsService;

    private final RedisService redisService;

    /**
     * Time, in seconds, after which a vehicle update is considered stale
     */
    private static final int gracePeriod = 5 * 60;
    private volatile FeedMessage tripUpdates = createFeedMessageBuilder().build();
    private volatile Map<String, FeedMessage> tripUpdatesByDatasource = Maps.newHashMap();
    private volatile FeedMessage vehiclePositions = createFeedMessageBuilder().build();
    private volatile Map<String, FeedMessage> vehiclePositionsByDatasource = Maps.newHashMap();
    private volatile FeedMessage alerts = createFeedMessageBuilder().build();
    private volatile Map<String, FeedMessage> alertsByDatasource = Maps.newHashMap();

    private final GtfsRtMapper gtfsMapper;

    public SiriToGtfsRealtimeService(@Autowired AlertFactory alertFactory,
                                     @Autowired RedisService redisService,
                                     @Autowired ServiceJourneyService serviceJourneyService,
                                     @Value("${kishar.datasource.et.whitelist}") List<String> datasourceETWhitelist,
                                     @Value("${kishar.datasource.vm.whitelist}") List<String> datasourceVMWhitelist,
                                     @Value("${kishar.datasource.sx.whitelist}") List<String> datasourceSXWhitelist,
                                     @Value("${kishar.settings.vm.close.to.stop.percentage}") int closeToNextStopPercentage,
                                     @Value("${kishar.settings.vm.close.to.stop.distance}") int closeToNextStopDistance) {
        this.datasourceETWhitelist = datasourceETWhitelist;
        this.datasourceVMWhitelist = datasourceVMWhitelist;
        this.datasourceSXWhitelist = datasourceSXWhitelist;
        this.alertFactory = alertFactory;
        this.redisService = redisService;
        this.gtfsMapper = new GtfsRtMapper(closeToNextStopPercentage, closeToNextStopDistance, serviceJourneyService);
    }

    @SuppressWarnings("unused")
    public void reset() {
        LOG.warn("Resetting ALL data");
        redisService.resetAllData();
    }

    @SuppressWarnings("unused")
    public String getStatus() {
        readLock.lock();
        try {
            ArrayList<String> status = new ArrayList<>();
            status.add("tripUpdates: " + tripUpdates.getEntityList().size());
            status.add("vehiclePositions: " + vehiclePositions.getEntityList().size());
            status.add("alerts: " + alerts.getEntityList().size());
            return status.toString();
        } finally {
            readLock.unlock();
        }
    }

    public Object getTripUpdates(String contentType, String datasource) {
        readLock.lock();
        try {
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
        } finally {
            readLock.unlock();
        }
    }

    public Object getVehiclePositions(String contentType, String datasource) {
        readLock.lock();
        try {
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
        } finally {
            readLock.unlock();
        }
    }

    public Object getAlerts(String contentType, String datasource) {
        readLock.lock();
        try {
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
        } finally {
            readLock.unlock();
        }
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

    private void checkPreconditions(VehicleActivityRecord vehicleActivity) {

        Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        CharSequence dataSourceRef = vehicleActivity.getMonitoredVehicleJourney().getDataSource();
        Preconditions.checkNotNull(dataSourceRef, "datasource");
        String datasource = dataSourceRef.toString();

        if (prometheusMetricsService != null) {
            boolean notInWhitelist = datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty() && !datasourceVMWhitelist.contains(datasource);
            prometheusMetricsService.registerIncomingEntity("SIRI_VM", notInWhitelist);
        }

        if (datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty()) {
            Preconditions.checkState(datasourceVMWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }

        if (vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef() != null) {
            checkPreconditions(vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef());
        } else {
            Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney().getVehicleRef());
        }


    }

    private void checkPreconditions(EstimatedVehicleJourneyRecord estimatedVehicleJourney) {

        CharSequence dataSourceRef = estimatedVehicleJourney.getDataSource();
        Preconditions.checkNotNull(dataSourceRef, "datasource");
        String datasource = dataSourceRef.toString();
        if (prometheusMetricsService != null) {
            boolean notInWhitelist = datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty() && !datasourceETWhitelist.contains(datasource);
            prometheusMetricsService.registerIncomingEntity("SIRI_ET", notInWhitelist);
        }

        if (datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty()) {
            Preconditions.checkState(datasourceETWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }

        if (estimatedVehicleJourney.getFramedVehicleJourneyRef() != null) {
            checkPreconditions(estimatedVehicleJourney.getFramedVehicleJourneyRef());
        } else if (Boolean.TRUE.equals(estimatedVehicleJourney.getExtraJourney())) {
            LOG.info("Ignoring - ExtraJourney not supported");
            throw new IllegalArgumentException("ExtraJourney not supported");
        } else {
            Preconditions.checkNotNull(estimatedVehicleJourney.getDatedVehicleJourneyRef());
        }

        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls(), "EstimatedCalls");
        Preconditions.checkState(!estimatedVehicleJourney.getEstimatedCalls().isEmpty(), "EstimatedCalls not empty");
    }

    private void checkPreconditions(PtSituationElementRecord situation) {
        Preconditions.checkNotNull(situation.getSituationNumber());

        Preconditions.checkNotNull(situation.getParticipantRef(), "datasource");
        String datasource = situation.getParticipantRef().toString();
        Preconditions.checkNotNull(datasource, "datasource");
        if (prometheusMetricsService != null) {
            boolean notInWhitelist = datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty() && !datasourceSXWhitelist.contains(datasource);
            prometheusMetricsService.registerIncomingEntity("SIRI_SX", notInWhitelist);
        }

        if (datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty()) {
            Preconditions.checkState(datasourceSXWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }
    }

    private void checkPreconditions(FramedVehicleJourneyRefRecord fvjRef) {
        Preconditions.checkNotNull(fvjRef.getDataFrameRef(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDatedVehicleJourneyRef(),"DatedVehicleJourneyRef");
        Preconditions.checkArgument(
                !fvjRef.getDatedVehicleJourneyRef().equals("null"),
                "DatedVehicleJourneyRef must not be the literal string 'null'");
    }

    private TripAndVehicleKey getKey(String tripId, String startDate, CharSequence vehicleRef) {

        String vehicle = null;
        if (vehicleRef != null) {
            vehicle = vehicleRef.toString();
        }

        return TripAndVehicleKey.fromTripIdServiceDateAndVehicleId(tripId, startDate, vehicle);
    }

    public void writeOutput() {
        long t1 = System.currentTimeMillis();
        writeTripUpdates();
        writeVehiclePositions();
        writeAlerts();
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerTotalGtfsRtEntities(tripUpdates.getEntityCount(), vehiclePositions.getEntityCount(), alerts.getEntityCount());
        }
        LOG.info("Wrote output in {} ms: {} alerts, {} vehicle-positions, {} trip-updates",
                (System.currentTimeMillis()-t1),
                alerts.getEntityCount(),
                vehiclePositions.getEntityCount(),
                tripUpdates.getEntityCount());
    }

    private record FeedOutput(FeedMessage feed, Map<String, FeedMessage> byDatasource) {}

    private FeedOutput buildFeedOutput(RedisService.Type type) {
        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> byDatasource = Maps.newHashMap();

        Map<String, byte[]> entityMap = redisService.readGtfsRtMap(type);

        for (Map.Entry<String, byte[]> entry : entityMap.entrySet()) {
            CompositeKey key = CompositeKey.create(entry.getKey());
            if (key == null) {
                continue;
            }
            FeedEntity entity;
            try {
                entity = FeedEntity.parseFrom(entry.getValue());
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Invalid feed entity from redis with key: {}", key, e);
                continue;
            }
            feedMessageBuilder.addEntity(entity);
            byDatasource
                .computeIfAbsent(key.getDatasource(), k -> createFeedMessageBuilder())
                .addEntity(entity);
        }

        return new FeedOutput(feedMessageBuilder.build(), buildFeedMessageMap(byDatasource));
    }

    private void writeTripUpdates() {
        FeedOutput output = buildFeedOutput(RedisService.Type.TRIP_UPDATE);
        setTripUpdates(output.feed(), output.byDatasource());
    }

    private Map<String, FeedMessage> buildFeedMessageMap(Map<String, FeedMessage.Builder> builders) {
        Map<String, FeedMessage> result = Maps.newHashMap();
        for (Map.Entry<String, FeedMessage.Builder> entry : builders.entrySet()) {
            result.put(entry.getKey(), entry.getValue().build());
        }
        return result;
    }

    private void writeVehiclePositions() {
        FeedOutput output = buildFeedOutput(RedisService.Type.VEHICLE_POSITION);
        setVehiclePositions(output.feed(), output.byDatasource());
    }

    private void writeAlerts() {
        FeedOutput output = buildFeedOutput(RedisService.Type.ALERT);
        setAlerts(output.feed(), output.byDatasource());
    }

    @SuppressWarnings("unused")
    public FeedMessage getTripUpdates() {
        readLock.lock();
        try {
            return tripUpdates;
        } finally {
            readLock.unlock();
        }
    }

    public void setTripUpdates(FeedMessage tripUpdates, Map<String, FeedMessage> tripUpdatesByDatasource) {
        writeLock.lock();
        try {
            this.tripUpdates = tripUpdates;
            this.tripUpdatesByDatasource = tripUpdatesByDatasource;
        } finally {
            writeLock.unlock();
        }
    }

    @SuppressWarnings("unused")
    public FeedMessage getVehiclePositions() {
        readLock.lock();
        try {
            return vehiclePositions;
        } finally {
            readLock.unlock();
        }
    }

    public void setVehiclePositions(FeedMessage vehiclePositions, Map<String, FeedMessage> vehiclePositionsByDatasource) {
        writeLock.lock();
        try {
            this.vehiclePositions = vehiclePositions;
            this.vehiclePositionsByDatasource = vehiclePositionsByDatasource;
        } finally {
            writeLock.unlock();
        }
    }

    @SuppressWarnings("unused")
    public FeedMessage getAlerts() {
        readLock.lock();
        try {
            return alerts;
        } finally {
            readLock.unlock();
        }
    }

    public void setAlerts(FeedMessage alerts, Map<String, FeedMessage> alertsByDatasource) {
        writeLock.lock();
        try {
            this.alerts = alerts;
            this.alertsByDatasource = alertsByDatasource;
        } finally {
            writeLock.unlock();
        }
    }

    public Map<String, GtfsRtData> convertSiriVmToGtfsRt(VehicleActivityRecord activity) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (activity != null) {
            try {
                checkPreconditions(activity);
            } catch (Exception e) {
               return result;
            }
            VehiclePosition.Builder builder = gtfsMapper.convertSiriToGtfsRt(activity);
            if (builder != null) {

                if (builder.getTimestamp() <= 0) {
                    builder.setTimestamp(Instant.now().getEpochSecond());
                }

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                String key = getKey(
                        builder.getTrip().getTripId(),
                        builder.getTrip().getStartDate(),
                        activity.getMonitoredVehicleJourney().getVehicleRef())
                        .toEntityId();
                entity.setId(key);

                entity.setVehicle(builder);

                Duration timeToLive;
                if (activity.getValidUntilTime() != null) {
                    timeToLive = Duration.newBuilder().setSeconds(
                            getInstant(activity.getValidUntilTime()).getEpochSecond() - Instant.now().getEpochSecond()
                    ).build();
                } else {
                    timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                }

                result.put(new CompositeKey(key, activity.getMonitoredVehicleJourney().getDataSource().toString()).asString(),
                        new GtfsRtData(entity.build().toByteArray(), timeToLive));
            }
        }

        return result;
    }

    public void registerGtfsRtVehiclePosition(Map<String, GtfsRtData> vehiclePositions) {
        redisService.writeGtfsRt(vehiclePositions, RedisService.Type.VEHICLE_POSITION);
    }

    public Map<String, GtfsRtData> convertSiriToGtfsRt(SiriRecord siri) {
        Map<String, GtfsRtData> result = Maps.newHashMap();
        if (siri != null &&
                siri.getServiceDelivery() != null) {
            ServiceDeliveryRecord serviceDelivery = siri.getServiceDelivery();
            // ET
            if (serviceDelivery.getEstimatedTimetableDeliveries() != null &&
                    !serviceDelivery.getEstimatedTimetableDeliveries().isEmpty()) {
                for (EstimatedTimetableDeliveryRecord delivery : serviceDelivery.getEstimatedTimetableDeliveries()) {
                    for (EstimatedJourneyVersionFrameRecord estimatedJourneyVersionFrame : delivery.getEstimatedJourneyVersionFrames()) {
                        for (EstimatedVehicleJourneyRecord estimatedVehicleJourney : estimatedJourneyVersionFrame.getEstimatedVehicleJourneys()) {
                            result.putAll(convertSiriEtToGtfsRt(estimatedVehicleJourney));
                        }
                    }
                }
            }

            // VM
            if (serviceDelivery.getVehicleMonitoringDeliveries() != null &&
                    !serviceDelivery.getVehicleMonitoringDeliveries().isEmpty()) {
                for (VehicleMonitoringDeliveryRecord estimatedTimetableDelivery : serviceDelivery.getVehicleMonitoringDeliveries()) {
                    for (VehicleActivityRecord vehicleActivityRecord : estimatedTimetableDelivery.getVehicleActivities()) {
                        result.putAll(convertSiriVmToGtfsRt(vehicleActivityRecord));
                    }
                }
            }

            // SX
            if (serviceDelivery.getSituationExchangeDeliveries() != null &&
                    !serviceDelivery.getSituationExchangeDeliveries().isEmpty()) {
                for (SituationExchangeDeliveryRecord estimatedTimetableDelivery : serviceDelivery.getSituationExchangeDeliveries()) {
                    for (PtSituationElementRecord situation : estimatedTimetableDelivery.getSituations()) {
                        result.putAll(convertSiriSxToGtfsRt(situation));
                    }
                }
            }
        }
        return result;
    }
    public Map<String, GtfsRtData> convertSiriEtToGtfsRt(EstimatedVehicleJourneyRecord estimatedVehicleJourney) {


        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (estimatedVehicleJourney != null) {
            try {
                checkPreconditions(estimatedVehicleJourney);
                TripUpdate.Builder builder = gtfsMapper.mapTripUpdateFromVehicleJourney(estimatedVehicleJourney);
                if (builder == null) {
                    return result;
                }

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                String key = TripAndVehicleKey.fromTripIdServiceDateAndVehicleId(
                        builder.getTrip().getTripId(),
                        builder.getTrip().getStartDate(),
                        null
                ).toEntityId();
                entity.setId(key);

                entity.setTripUpdate(builder);

                Instant expirationTime = null;
                for (RecordedCallRecord call : estimatedVehicleJourney.getRecordedCalls()) {
                    if (call.getActualArrivalTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getActualArrivalTime()));
                    } else if (call.getExpectedArrivalTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getExpectedArrivalTime()));
                    } else if (call.getAimedArrivalTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getAimedArrivalTime()));
                    } else if (call.getActualDepartureTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getActualDepartureTime()));
                    } else if (call.getExpectedDepartureTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getExpectedDepartureTime()));
                    }else if (call.getAimedDepartureTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getAimedDepartureTime()));
                    }
                }
                for (EstimatedCallRecord call : estimatedVehicleJourney.getEstimatedCalls()) {
                    if (call.getExpectedArrivalTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getExpectedArrivalTime()));
                    } else if (call.getAimedArrivalTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getAimedArrivalTime()));
                    } else if (call.getExpectedDepartureTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getExpectedDepartureTime()));
                    }else if (call.getAimedDepartureTime() != null) {
                        expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, getInstant(call.getAimedDepartureTime()));
                    }
                }

                Duration timeToLive;
                if (expirationTime == null) {
                    timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                } else {
                    timeToLive = Duration.newBuilder().setSeconds(
                            expirationTime.getEpochSecond() - Instant.now().getEpochSecond()
                    ).build();
                }

                result.put(
                        new CompositeKey(
                                key,
                                estimatedVehicleJourney.getDataSource().toString()
                        ).asString(), new GtfsRtData(entity.build().toByteArray(), timeToLive));
            } catch (IllegalStateException e) {
                String id = estimatedVehicleJourney.getDataSource() != null
                        ? estimatedVehicleJourney.getDataSource().toString()
                        : "unknown";
                LOG.debug("Skipping trip update from {}: {}", id, e.getMessage());
            } catch (Exception e) {
                String id = estimatedVehicleJourney.getDataSource() != null
                        ? estimatedVehicleJourney.getDataSource().toString()
                        : "unknown";
                LOG.warn("Failed parsing trip update from {}", id, e);
            }
        }

        return result;
    }

    public void registerGtfsRtTripUpdates(Map<String, GtfsRtData> tripUpdates) {
        redisService.writeGtfsRt(tripUpdates, RedisService.Type.TRIP_UPDATE);
    }

    public Map<String, GtfsRtData> convertSiriSxToGtfsRt(PtSituationElementRecord ptSituationElement) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (ptSituationElement != null) {
            try {
                checkPreconditions(ptSituationElement);
                Alert.Builder alertFromSituation = alertFactory.createAlertFromSituation(ptSituationElement);

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                String key = ptSituationElement.getSituationNumber().toString();
                entity.setId(key);

                entity.setAlert(alertFromSituation);

                Instant endTime = null;
                for (ValidityPeriodRecord range : ptSituationElement.getValidityPeriods()) {
                    if (range.getEndTime() == null) {
                        continue;
                    }
                    Instant rangeEndTimestamp = getInstant(range.getEndTime());
                    endTime = SiriLibrary.getLatestTimestamp(endTime, rangeEndTimestamp);
                }
                Duration timeToLive;
                if (endTime != null) {
                    timeToLive = Duration.newBuilder().setSeconds(
                            endTime.getEpochSecond() - Instant.now().getEpochSecond()
                    ).build();
                } else {
                    timeToLive = Duration.newBuilder().setSeconds(3600*24*365).build();
                }

                result.put(
                        new CompositeKey(
                                key,
                                ptSituationElement.getParticipantRef().toString()).asString(),
                        new GtfsRtData(entity.build().toByteArray(), timeToLive));
            } catch (IllegalStateException e) {
                LOG.info("Failed parsing alert {}: {}", ptSituationElement.getSituationNumber(), e.getMessage());
            } catch (Exception e) {
                LOG.warn("Failed parsing alerts", e);
            }
        }

        return result;
    }

    public void registerGtfsRtAlerts(Map<String, GtfsRtData> alerts) {
        redisService.writeGtfsRt(alerts, RedisService.Type.ALERT);
    }
}
