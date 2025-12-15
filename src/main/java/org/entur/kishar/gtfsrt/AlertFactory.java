/**
 * Copyright (C) 2011 Google, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime.Alert;
import com.google.transit.realtime.GtfsRealtime.EntitySelector;
import com.google.transit.realtime.GtfsRealtime.TimeRange;
import com.google.transit.realtime.GtfsRealtime.TranslatedString;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import org.entur.avro.realtime.siri.model.AffectedLineRecord;
import org.entur.avro.realtime.siri.model.AffectedNetworkRecord;
import org.entur.avro.realtime.siri.model.AffectedRouteRecord;
import org.entur.avro.realtime.siri.model.AffectedStopPlaceRecord;
import org.entur.avro.realtime.siri.model.AffectedStopPointRecord;
import org.entur.avro.realtime.siri.model.AffectedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.AffectsRecord;
import org.entur.avro.realtime.siri.model.FramedVehicleJourneyRefRecord;
import org.entur.avro.realtime.siri.model.InfoLinkRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.StopPointsRecord;
import org.entur.avro.realtime.siri.model.ValidityPeriodRecord;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.entur.kishar.gtfsrt.helpers.graphql.model.ServiceJourney;
import org.entur.kishar.gtfsrt.mappers.AvroHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Service
public class AlertFactory extends AvroHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlertFactory.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Autowired
    ServiceJourneyService serviceJourneyService;

    public Alert.Builder createAlertFromSituation(
            PtSituationElementRecord ptSituation) {

        Alert.Builder alert = Alert.newBuilder();

        handleDescriptions(ptSituation, alert);
        handleValidityPeriod(ptSituation, alert);
//        handleReasons(ptSituation, alert);
        handleAffects(ptSituation, alert);
        if (alert.getInformedEntityCount() == 0) {
            LOGGER.info("No informed entities found/matched for alert {}", ptSituation.getSituationNumber());
        }
//        handleConsequences(ptSituation, alert);
        handleUrls(ptSituation, alert);

        return alert;
    }

    private void handleDescriptions(PtSituationElementRecord ptSituation,
                                    Alert.Builder serviceAlert) {

        TranslatedString summary = translation(ptSituation.getSummaries());
        if (summary != null) {
            serviceAlert.setHeaderText(summary);
        }

        TranslatedString description = translation(ptSituation.getDescriptions());
        if (description != null) {
            serviceAlert.setDescriptionText(description);
        }
    }

    private void handleValidityPeriod(PtSituationElementRecord ptSituation,
                                   Alert.Builder serviceAlert) {

        List<ValidityPeriodRecord> validityPeriods = ptSituation.getValidityPeriods();
        if (validityPeriods != null) {
            for (ValidityPeriodRecord validityPeriod : validityPeriods) {
                TimeRange.Builder timeRange = TimeRange.newBuilder();
                if (validityPeriod.getStartTime() != null) {
                    timeRange.setStart(getInstant(validityPeriod.getStartTime()).getEpochSecond());
                }
                if (validityPeriod.getEndTime() != null) {
                    timeRange.setEnd(getInstant(validityPeriod.getEndTime()).getEpochSecond());
                } else {
                    timeRange.setEnd(Instant.now().plus(365, ChronoUnit.DAYS).getEpochSecond());
                }

                if (timeRange.hasStart() || timeRange.hasEnd()) {
                    serviceAlert.addActivePeriod(timeRange);
                }
            }
        }
    }

    /****
     * Affects
     ****/

    private void handleAffects(PtSituationElementRecord ptSituation,
                               Alert.Builder serviceAlert) {

        AffectsRecord affectsRecord = ptSituation.getAffects();

        if (affectsRecord == null) {
            return;
        }

        if (affectsRecord.getStopPoints() != null && !affectsRecord.getStopPoints().isEmpty()) {
            List<AffectedStopPointRecord> stopPoints = affectsRecord.getStopPoints();

            if (stopPoints != null) {

                for (AffectedStopPointRecord stopPoint : stopPoints) {
                    if (stopPoint.getStopPointRef() == null) {
                        continue;
                    }
                    String stopRef =  stopPoint.getStopPointRef().toString();
                    serviceAlert.addInformedEntity(
                            EntitySelector.newBuilder()
                                    .setStopId(stopRef)
                                    .build()
                    );
                }
            }
        }

        if (affectsRecord.getVehicleJourneys() != null && !affectsRecord.getVehicleJourneys().isEmpty()) {
            List<AffectedVehicleJourneyRecord> vehicleJourneys = affectsRecord.getVehicleJourneys();
            if (vehicleJourneys != null && !vehicleJourneys.isEmpty()) {

                for (AffectedVehicleJourneyRecord affectedVehicleJourney : vehicleJourneys) {

                    String routeId = null;
                    List<TripDescriptor> tripDescriptors = new ArrayList<>();
                    List<String> stopIds = new ArrayList<>();

                    if (affectedVehicleJourney.getLineRef() != null) {
                        routeId = affectedVehicleJourney.getLineRef().toString();
                    }

                    String startDate = null;
                    if (affectedVehicleJourney.getOriginAimedDepartureTime() != null) {

                        startDate = DATE_FORMATTER.format(
                                LocalDate.ofInstant(getInstant(affectedVehicleJourney.getOriginAimedDepartureTime()),
                                        ZoneId.systemDefault())
                        );
                    }

                    if (affectedVehicleJourney.getVehicleJourneyRefs() != null && !affectedVehicleJourney.getVehicleJourneyRefs().isEmpty()) {

                        List<CharSequence> vehicleJourneyRefs = affectedVehicleJourney.getVehicleJourneyRefs();
                        for (CharSequence tripRef : vehicleJourneyRefs) {
                            TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
                            tripDescriptor.setTripId(tripRef.toString());
                            if (routeId != null) {
                                tripDescriptor.setRouteId(routeId);
                            }
                            if (startDate != null) {
                                tripDescriptor.setStartDate(startDate);
                            }
                            tripDescriptors.add(tripDescriptor.build());
                        }
                    }

                    if (affectedVehicleJourney.getFramedVehicleJourneyRef() != null){
                        final FramedVehicleJourneyRefRecord framedVehicleJourneyRef = affectedVehicleJourney.getFramedVehicleJourneyRef();
                        final String datedVehicleJourneyRef =
                                framedVehicleJourneyRef.getDatedVehicleJourneyRef().toString();
                        final String dataFrameRef = framedVehicleJourneyRef.getDataFrameRef().toString();

                        TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
                        tripDescriptor.setTripId(datedVehicleJourneyRef);
                        if (!dataFrameRef.isBlank()) {
                            // Convert YYYY-MM-MM to YYYYMMDD
                            String dataFrameStr = dataFrameRef.replaceAll("-","");
                            tripDescriptor.setStartDate(dataFrameStr);
                        }
                        tripDescriptors.add(tripDescriptor.build());
                    }

                    if (affectedVehicleJourney.getDatedVehicleJourneyRefs() != null && !affectedVehicleJourney.getDatedVehicleJourneyRefs().isEmpty()) {
                        for (CharSequence datedVehicleJourneyRef : affectedVehicleJourney.getDatedVehicleJourneyRefs()) {
                            if (datedVehicleJourneyRef != null) {
                                String datedVehicleJourneyRefStr = datedVehicleJourneyRef.toString();
                                ServiceJourney serviceJourney = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(datedVehicleJourneyRefStr);
                                if (serviceJourney != null && serviceJourney.getId() != null) {
                                    TripDescriptor.Builder builder = TripDescriptor.newBuilder()
                                                                            .setTripId(serviceJourney.getId());
                                    if (serviceJourney.getDate() != null) {
                                        builder.setStartDate(serviceJourney.getDate().replaceAll("-",""));
                                    }
                                    tripDescriptors.add(builder.build());
                                }
                            }
                        }
                    }

                    if (affectedVehicleJourney.getRoutes() != null) {
                        for (AffectedRouteRecord affectedRoute : affectedVehicleJourney.getRoutes()) {
                            if (affectedRoute.getStopPoints() != null) {
                                StopPointsRecord stopPoints = affectedRoute.getStopPoints();
                                if (stopPoints.getStopPoints() != null) {

                                    for (AffectedStopPointRecord stopPoint : stopPoints.getStopPoints()) {
                                        if (stopPoint.getStopPointRef() != null) {
                                            stopIds.add(stopPoint.getStopPointRef().toString());
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (tripDescriptors.isEmpty()) {
                        // Route only
                        if (routeId != null) {
                            EntitySelector.Builder selector = EntitySelector.newBuilder();
                            selector.setRouteId(routeId);
                            serviceAlert.addInformedEntity(selector);
                        }
                    } else {
                        for (TripDescriptor tripDescriptor : tripDescriptors) {
                            if (stopIds.isEmpty()) {
                                // Trip only
                                EntitySelector.Builder selector = EntitySelector.newBuilder();
                                selector.setTrip(tripDescriptor);
                                serviceAlert.addInformedEntity(selector);
                            } else {
                                for (String stopId : stopIds) {
                                    // One for each trip/stop combination
                                    EntitySelector.Builder selector = EntitySelector.newBuilder();
                                    selector.setTrip(tripDescriptor);
                                    selector.setStopId(stopId);
                                    serviceAlert.addInformedEntity(selector);
                                }
                            }
                        }
                    }
                }
            }
        }

        if (affectsRecord.getNetworks() != null && !affectsRecord.getNetworks().isEmpty()) {
            List<AffectedNetworkRecord> networks = affectsRecord.getNetworks();
            for (AffectedNetworkRecord affectedNetwork : networks) {

                if (affectedNetwork.getAffectedLines() != null) {
                    for (AffectedLineRecord affectedLine : affectedNetwork.getAffectedLines()) {

                        CharSequence lineRef = "";
                        if (affectedLine.getLineRef() != null) {
                            lineRef = affectedLine.getLineRef();
                        }
                        if (affectedLine.getRoutes() != null) {
                            List<AffectedRouteRecord> routes = affectedLine.getRoutes();
                            if (routes != null) {
                                for (AffectedRouteRecord affectedRoute : routes) {
                                    if (affectedRoute.getStopPoints() != null) {
                                        StopPointsRecord stopPoints = affectedRoute.getStopPoints();
                                        if (stopPoints.getStopPoints() != null) {
                                            for (AffectedStopPointRecord stopPoint : stopPoints.getStopPoints()) {
                                                if (stopPoint.getStopPointRef() != null) {
                                                    EntitySelector.Builder selector = EntitySelector.newBuilder();
                                                    if (lineRef != null) {
                                                        selector.setRouteId(lineRef.toString());
                                                    }
                                                    selector.setStopId(
                                                            stopPoint.getStopPointRef().toString()
                                                    );

                                                    serviceAlert.addInformedEntity(selector);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            EntitySelector.Builder selector = EntitySelector.newBuilder();
                            selector.setRouteId(lineRef.toString());
                            serviceAlert.addInformedEntity(selector);
                        }
                    }
                }
            }

        }

        if (affectsRecord.getStopPlaces() != null && !affectsRecord.getStopPlaces().isEmpty()) {
            List<AffectedStopPlaceRecord> stopPlaces = affectsRecord.getStopPlaces();
            for (AffectedStopPlaceRecord affectedStopPlace : stopPlaces) {
                if (affectedStopPlace.getStopPlaceRef() != null) {
                    final String stopPlaceRef = affectedStopPlace.getStopPlaceRef().toString();

                    EntitySelector.Builder selector = EntitySelector.newBuilder();
                    selector.setStopId(stopPlaceRef);
                    serviceAlert.addInformedEntity(selector);
                }
            }
        }
    }

    private void handleUrls(PtSituationElementRecord ptSituation, Alert.Builder alert) {
        List<InfoLinkRecord> infoLinks = ptSituation.getInfoLinks();
        if (infoLinks != null) {
            TranslatedString.Builder url = TranslatedString.newBuilder();
            for (InfoLinkRecord infoLinkRecord : infoLinks) {
                TranslatedString.Translation.Builder translation = TranslatedString.Translation.newBuilder();
                translation.setText(infoLinkRecord.getUri().toString());
                url.addTranslation(translation);
            }
            alert.setUrl(url);
        }
    }
}
