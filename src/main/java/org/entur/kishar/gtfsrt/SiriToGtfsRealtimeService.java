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
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.createFeedMessageBuilder;
import static org.entur.kishar.gtfsrt.mappers.AvroHelper.getInstant;

@Service
@Configuration
public class SiriToGtfsRealtimeService {
    private static final Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

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
    private FeedMessage tripUpdates = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> tripUpdatesByDatasource = Maps.newHashMap();
    private FeedMessage vehiclePositions = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> vehiclePositionsByDatasource = Maps.newHashMap();
    private FeedMessage alerts = createFeedMessageBuilder().build();
    private Map<String, FeedMessage> alertsByDatasource = Maps.newHashMap();

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
        ArrayList<String> status = new ArrayList<>();
        status.add("tripUpdates: " + tripUpdates.getEntityList().size());
        status.add("vehiclePositions: " + vehiclePositions.getEntityList().size());
        status.add("alerts: " + alerts.getEntityList().size());
        return status.toString();
    }

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

    private void checkPreconditions(VehicleActivityRecord vehicleActivity) {

        Preconditions.checkNotNull(vehicleActivity.getMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        String datasource = vehicleActivity.getMonitoredVehicleJourney().getDataSource().toString();
        Preconditions.checkNotNull(datasource, "datasource");

        if (prometheusMetricsService != null) {
            boolean notInWhitelist = datasourceVMWhitelist != null && !datasourceVMWhitelist.isEmpty() && !datasourceVMWhitelist.contains(datasource);
            prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, notInWhitelist);
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

        String datasource = estimatedVehicleJourney.getDataSource().toString();
        Preconditions.checkNotNull(datasource, "datasource");
        if (prometheusMetricsService != null) {
            boolean notInWhitelist = datasourceETWhitelist != null && !datasourceETWhitelist.isEmpty() && !datasourceETWhitelist.contains(datasource);
            prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, notInWhitelist);
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
            prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, notInWhitelist);
        }

        if (datasourceSXWhitelist != null && !datasourceSXWhitelist.isEmpty()) {
            Preconditions.checkState(datasourceSXWhitelist.contains(datasource), "datasource " + datasource + " must be in the whitelist");
        }
    }

    private void checkPreconditions(FramedVehicleJourneyRefRecord fvjRef) {
        Preconditions.checkNotNull(fvjRef.getDataFrameRef(),"DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDatedVehicleJourneyRef(),"DatedVehicleJourneyRef");
        Preconditions.checkArgument(
                fvjRef.getDatedVehicleJourneyRef() != null && !fvjRef.getDatedVehicleJourneyRef().equals("null"),
                "DatedVehicleJourneyRef");
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

    private void writeTripUpdates() {

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        Map<String, byte[]> tripUpdateMap = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);

        for (String keyBytes : tripUpdateMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);

            if (key == null) {
                continue;
            }

            FeedEntity entity;
            try {
                byte[] data = tripUpdateMap.get(keyBytes);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from reddis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

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

    private String getTripIdForEstimatedVehicleJourney(TripUpdate.Builder tripUpdateBuilder) {
        StringBuilder b = new StringBuilder();

        b.append((tripUpdateBuilder.getTrip().getTripId()));
        b.append('-');
        b.append(tripUpdateBuilder.getTrip().getStartDate());
        if (tripUpdateBuilder.getVehicle() != null) {
            b.append('-');
            b.append(tripUpdateBuilder.getVehicle().getId());
        }
        return b.toString();
    }

    private void writeVehiclePositions() {

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        Map<String, byte[]> vehiclePositionMap = redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION);

        for (String keyBytes : vehiclePositionMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);

            if (key == null) {
                continue;
            }

            FeedEntity entity;
            try {
                byte[] data = vehiclePositionMap.get(keyBytes);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from redis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();

            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);

            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            feedMessageBuilder.addEntity(entity);
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
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

        Map<String, byte[]> alertMap = redisService.readGtfsRtMap(RedisService.Type.ALERT);

        for (String keyBytes : alertMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);

            if (key == null) {
                continue;
            }

            FeedEntity entity;
            try {
                byte[] data = alertMap.get(keyBytes);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from reddis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            feedMessageBuilder.addEntity(entity);
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }

        setAlerts(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
    }

    @SuppressWarnings("unused")
    public FeedMessage getTripUpdates() {
        return tripUpdates;
    }

    public void setTripUpdates(FeedMessage tripUpdates, Map<String, FeedMessage> tripUpdatesByDatasource) {
        this.tripUpdates = tripUpdates;
        this.tripUpdatesByDatasource = tripUpdatesByDatasource;
    }

    @SuppressWarnings("unused")
    public FeedMessage getVehiclePositions() {
        return vehiclePositions;
    }

    public void setVehiclePositions(FeedMessage vehiclePositions, Map<String, FeedMessage> vehiclePositionsByDatasource) {
        this.vehiclePositions = vehiclePositions;
        this.vehiclePositionsByDatasource = vehiclePositionsByDatasource;
    }

    @SuppressWarnings("unused")
    public FeedMessage getAlerts() {
        return alerts;
    }

    public void setAlerts(FeedMessage alerts, Map<String, FeedMessage> alertsByDatasource) {
        this.alerts = alerts;
        this.alertsByDatasource = alertsByDatasource;
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
                    builder.setTimestamp(System.currentTimeMillis());
                }

                FeedEntity.Builder entity = FeedEntity.newBuilder();
                String key = getVehicleIdForKey(getKey(
                        builder.getTrip().getTripId(),
                        builder.getTrip().getStartDate(),
                        activity.getMonitoredVehicleJourney().getVehicleRef())
                );
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
                String key = getTripIdForEstimatedVehicleJourney(builder);
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
            } catch (Exception e) {
                //LOG.debug("Failed parsing trip updates", e);
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
