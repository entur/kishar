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

import com.google.protobuf.Timestamp;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.Alert.Cause;
import com.google.transit.realtime.GtfsRealtime.Alert.Effect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uk.org.ifopt.www.ifopt.StopPlaceRefStructure;
import uk.org.siri.www.siri.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.translation;

@Service
public class AlertFactory {

    private static final Logger _log = LoggerFactory.getLogger(AlertFactory.class);
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("HH:mm:ss");

    public Alert.Builder createAlertFromSituation(
            PtSituationElementStructure ptSituation) {

        Alert.Builder alert = Alert.newBuilder();

        handleDescriptions(ptSituation, alert);
        handleValidityPeriod(ptSituation, alert);
        handleReasons(ptSituation, alert);
        handleAffects(ptSituation, alert);
        handleConsequences(ptSituation, alert);
        handleUrls(ptSituation, alert);

        return alert;
    }

    private void handleUrls(PtSituationElementStructure ptSituation, Alert.Builder alert) {
        if (ptSituation.hasInfoLinks()) {
            final PtSituationElementStructure.InfoLinksType infoLinks = ptSituation.getInfoLinks();
            if (infoLinks.getInfoLinkCount() > 0) {
                TranslatedString.Builder url = TranslatedString.newBuilder();
                for (InfoLinkStructure infoLinkStructure : infoLinks.getInfoLinkList()) {
                    TranslatedString.Translation.Builder translation = TranslatedString.Translation.newBuilder();
                    translation.setText(infoLinkStructure.getUri());
                    url.addTranslation(translation);
                }
                alert.setUrl(url);
            }
        }
    }

    private void handleDescriptions(PtSituationElementStructure ptSituation,
                                    Alert.Builder serviceAlert) {

        TranslatedString summary = translation(ptSituation.getSummaryList());
        if (summary != null) {
            serviceAlert.setHeaderText(summary);
        }

        TranslatedString description = translation(ptSituation.getDescriptionList());
        if (description != null) {
            serviceAlert.setDescriptionText(description);
        }
    }

    private void handleValidityPeriod(PtSituationElementStructure ptSituation,
                                   Alert.Builder serviceAlert) {

        if (!ptSituation.getValidityPeriodList().isEmpty()) {
            final List<HalfOpenTimestampOutputRangeStructure> validityPeriodList = ptSituation.getValidityPeriodList();
            for (HalfOpenTimestampOutputRangeStructure validityPeriod : validityPeriodList) {
                TimeRange.Builder timeRange = TimeRange.newBuilder();
                if (validityPeriod.hasStartTime()) {
                    timeRange.setStart(validityPeriod.getStartTime().getSeconds());
                }
                if (validityPeriod.hasEndTime()) {
                    timeRange.setEnd(validityPeriod.getEndTime().getSeconds());
                }

                if (timeRange.hasStart() || timeRange.hasEnd()) {
                    serviceAlert.addActivePeriod(timeRange);
                }
            }
        }
    }

    private void handleReasons(PtSituationElementStructure ptSituation,
                              Alert.Builder serviceAlert) {

        Cause cause = getReasonAsCause(ptSituation);
        if (cause != null) {
            serviceAlert.setCause(cause);
        }
    }

    private Cause getReasonAsCause(PtSituationElementStructure ptSituation) {
        if (ptSituation.getEnvironmentReason() != null &&
            ptSituation.getEnvironmentReasonValue() > 0) {
            return Cause.WEATHER;
        }
        if (ptSituation.getEquipmentReason() != null &&
            ptSituation.getEquipmentReasonValue() > 0) {
            switch (ptSituation.getEquipmentReason()) {
                case EQUIPMENT_REASON_ENUMERATION_CONSTRUCTION_WORK:
                    return Cause.CONSTRUCTION;
                case EQUIPMENT_REASON_ENUMERATION_CLOSED_FOR_MAINTENANCE:
                case EQUIPMENT_REASON_ENUMERATION_MAINTENANCE_WORK:
                case EQUIPMENT_REASON_ENUMERATION_EMERGENCY_ENGINEERING_WORK:
                case EQUIPMENT_REASON_ENUMERATION_LATE_FINISH_TO_ENGINEERING_WORK:
                case EQUIPMENT_REASON_ENUMERATION_REPAIR_WORK:
                    return Cause.MAINTENANCE;
            }
        }
        if (ptSituation.getPersonnelReason() != null &&
            ptSituation.getPersonnelReasonValue() > 0) {
            switch (ptSituation.getPersonnelReason()) {
                case PERSONNEL_REASON_ENUMERATION_INDUSTRIAL_ACTION:
                case PERSONNEL_REASON_ENUMERATION_UNOFFICIAL_INDUSTRIAL_ACTION:
                    return Cause.STRIKE;
            }
            return Cause.OTHER_CAUSE;
        }
        /**
         * There are really so many possibilities here that it's tricky to translate
         * them all
         */
        if (ptSituation.getMiscellaneousReason() != null &&
            ptSituation.getMiscellaneousReasonValue() > 0) {
            switch (ptSituation.getMiscellaneousReason()) {
                case MISCELLANEOUS_REASON_ENUMERATION_ACCIDENT:
                case MISCELLANEOUS_REASON_ENUMERATION_COLLISION:
                    return Cause.ACCIDENT;
                case MISCELLANEOUS_REASON_ENUMERATION_DEMONSTRATION:
                case MISCELLANEOUS_REASON_ENUMERATION_MARCH:
                    return Cause.DEMONSTRATION;
                case MISCELLANEOUS_REASON_ENUMERATION_PERSON_ILL_ON_VEHICLE:
                case MISCELLANEOUS_REASON_ENUMERATION_FATALITY:
                    return Cause.MEDICAL_EMERGENCY;
                case MISCELLANEOUS_REASON_ENUMERATION_POLICE_REQUEST:
                case MISCELLANEOUS_REASON_ENUMERATION_BOMB_ALERT:
                case MISCELLANEOUS_REASON_ENUMERATION_CIVIL_EMERGENCY:
                case MISCELLANEOUS_REASON_ENUMERATION_EMERGENCY_SERVICES:
                case MISCELLANEOUS_REASON_ENUMERATION_EMERGENCY_SERVICES_CALL:
                    return Cause.POLICE_ACTIVITY;
            }
        }

        return null;
    }

    /****
     * Affects
     ****/

    private void handleAffects(PtSituationElementStructure ptSituation,
                               Alert.Builder serviceAlert) {

        AffectsScopeStructure affectsStructure = ptSituation.getAffects();

        if (affectsStructure == null) {
            return;
        }


        if (affectsStructure.hasOperators()) {
            AffectsScopeStructure.OperatorsType operators = affectsStructure.getOperators();
            if (operators != null && operators.getAffectedOperatorCount() > 0) {

                for (AffectedOperatorStructure operator : operators.getAffectedOperatorList()) {
                    OperatorRefStructure operatorRef = operator.getOperatorRef();
                    if (operatorRef == null || operatorRef.getValue() == null) {
                        continue;
                    }
                    String agencyId = (operatorRef.getValue());
                    EntitySelector.Builder selector = EntitySelector.newBuilder();
                    selector.setAgencyId(agencyId);
                    serviceAlert.addInformedEntity(selector);
                }
            }
        }

        if (affectsStructure.hasStopPoints()) {
            AffectsScopeStructure.StopPointsType stopPoints = affectsStructure.getStopPoints();

            if (stopPoints != null && stopPoints.getAffectedStopPointCount() > 0) {

                for (AffectedStopPointStructure stopPoint : stopPoints.getAffectedStopPointList()) {
                    StopPointRefStructure stopRef = stopPoint.getStopPointRef();
                    if (stopRef == null || stopRef.getValue() == null) {
                        continue;
                    }
                    String stopId = (stopRef.getValue());
                    EntitySelector.Builder selector = EntitySelector.newBuilder();
                    selector.setStopId(stopId);
                    serviceAlert.addInformedEntity(selector);
                }
            }
        }

        if (affectsStructure.hasVehicleJourneys()) {
            AffectsScopeStructure.VehicleJourneysType vjs = affectsStructure.getVehicleJourneys();
            if (vjs != null && vjs.getAffectedVehicleJourneyCount() > 0) {

                for (AffectedVehicleJourneyStructure affectedVehicleJourney : vjs.getAffectedVehicleJourneyList()) {

                    String routeId = null;
                    List<TripDescriptor> tripDescriptors = new ArrayList<>();
                    List<String> stopIds = new ArrayList<>();

                    if (affectedVehicleJourney.hasLineRef()) {
                        routeId = (affectedVehicleJourney.getLineRef().getValue());
                    }

                    String startDate = null;
                    String startTime = null;
                    if (affectedVehicleJourney.hasOriginAimedDepartureTime()) {
                        final Timestamp originAimedDepartureTime = affectedVehicleJourney.getOriginAimedDepartureTime();
                        Date date = new Date(originAimedDepartureTime.getSeconds()*1000);

                        startDate = DATE_FORMATTER.format(date);
                        startTime = TIME_FORMATTER.format(date);
                    }

                    List<VehicleJourneyRefStructure> tripRefs = affectedVehicleJourney.getVehicleJourneyRefList();
                    for (VehicleJourneyRefStructure tripRef : tripRefs) {
                        TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
                        tripDescriptor.setTripId(tripRef.getValue());
                        if (routeId != null) {
                            tripDescriptor.setRouteId(routeId);
                        }
                        if (startDate != null) {
                            tripDescriptor.setStartDate(startDate);
                        }
                        if (startTime != null) {
                            tripDescriptor.setStartTime(startTime);
                        }
                        tripDescriptors.add(tripDescriptor.build());
                    }

                    if (affectedVehicleJourney.getRouteCount() > 0) {
                        for (AffectedRouteStructure affectedRoute : affectedVehicleJourney.getRouteList()) {
                            if (affectedRoute.hasStopPoints()) {
                                final AffectedRouteStructure.StopPointsType stopPoints = affectedRoute.getStopPoints();
                                if (stopPoints.getSequenceWrapperCount() > 0) {
                                    for (AffectedRouteStructure.StopPointsType.SequenceWrapper_StopPointsType stopPoint : stopPoints.getSequenceWrapperList()) {
                                        if (stopPoint.hasAffectedStopPoint()) {
                                            final String stopId = stopPoint
                                                .getAffectedStopPoint()
                                                .getStopPointRef()
                                                .getValue();

                                            stopIds.add(stopId);
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

        if (affectsStructure.hasNetworks()) {
            final AffectsScopeStructure.NetworksType networks = affectsStructure.getNetworks();
            if (networks.getAffectedNetworkCount() > 0) {
                for (AffectsScopeStructure.NetworksType.AffectedNetworkType affectedNetwork : networks.getAffectedNetworkList()) {

                    if (affectedNetwork.getAffectedLineCount() > 0) {
                        for (AffectedLineStructure affectedLine : affectedNetwork.getAffectedLineList()) {

                            String lineRef = null;
                            if (affectedLine.hasLineRef()) {
                                lineRef = affectedLine.getLineRef().getValue();
                            }
                            if (affectedLine.hasRoutes()) {
                                final AffectedLineStructure.RoutesType routes = affectedLine.getRoutes();
                                if (routes.getAffectedRouteCount() > 0) {
                                    for (AffectedRouteStructure affectedRoute : routes.getAffectedRouteList()) {
                                        if (affectedRoute.hasStopPoints()) {
                                            final AffectedRouteStructure.StopPointsType stopPoints = affectedRoute.getStopPoints();
                                            if (stopPoints.getSequenceWrapperCount() > 0) {
                                                for (AffectedRouteStructure.StopPointsType.SequenceWrapper_StopPointsType stopPoint : stopPoints.getSequenceWrapperList()) {
                                                    if (stopPoint.hasAffectedStopPoint()) {
                                                        EntitySelector.Builder selector = EntitySelector.newBuilder();
                                                        if (lineRef != null) {
                                                            selector.setRouteId(lineRef);
                                                        }
                                                        selector.setStopId(stopPoint.getAffectedStopPoint().getStopPointRef().getValue());

                                                        serviceAlert.addInformedEntity(selector);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                EntitySelector.Builder selector = EntitySelector.newBuilder();
                                selector.setRouteId(lineRef);
                                serviceAlert.addInformedEntity(selector);
                            }
                        }
                    }
                }
            }
        }

        if (affectsStructure.hasStopPlaces()) {
            final AffectsScopeStructure.StopPlacesType stopPlaces = affectsStructure.getStopPlaces();
            if (stopPlaces.getAffectedStopPlaceCount() > 0) {
                for (AffectedStopPlaceStructure affectedStopPlace : stopPlaces.getAffectedStopPlaceList()) {
                    if (affectedStopPlace.hasStopPlaceRef()) {
                        final StopPlaceRefStructure stopPlaceRef = affectedStopPlace.getStopPlaceRef();

                        EntitySelector.Builder selector = EntitySelector.newBuilder();
                        selector.setStopId(stopPlaceRef.getValue());
                        serviceAlert.addInformedEntity(selector);
                    }
                }
            }
        }
    }

    private void handleConsequences(PtSituationElementStructure ptSituation,
                                    Alert.Builder serviceAlert) {

        PtConsequencesStructure consequences = ptSituation.getConsequences();

        if (consequences == null || consequences.getConsequenceCount() == 0) {
            return;
        }

        for (PtConsequenceStructure consequence : consequences.getConsequenceList()) {
            if (consequence.getConditionCount() > 0) {
                serviceAlert.setEffect(getConditionAsEffect(consequence.getConditionList()));
            }
        }
    }

    private Effect getConditionAsEffect(List<ServiceConditionEnumeration> conditions) {
        for (ServiceConditionEnumeration condition : conditions) {

            switch (condition) {

                case SERVICE_CONDITION_ENUMERATION_CANCELLED:
                case SERVICE_CONDITION_ENUMERATION_NO_SERVICE:
                    return Effect.NO_SERVICE;

                case SERVICE_CONDITION_ENUMERATION_DELAYED:
                    return Effect.SIGNIFICANT_DELAYS;

                case SERVICE_CONDITION_ENUMERATION_DIVERTED:
                    return Effect.DETOUR;

                case SERVICE_CONDITION_ENUMERATION_ADDITIONAL_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_EXTENDED_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_SHUTTLE_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_SPECIAL_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_REPLACEMENT_SERVICE:
                    return Effect.ADDITIONAL_SERVICE;

                case SERVICE_CONDITION_ENUMERATION_DISRUPTED:
                case SERVICE_CONDITION_ENUMERATION_INTERMITTENT_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_SHORT_FORMED_SERVICE:
                    return Effect.REDUCED_SERVICE;

                case SERVICE_CONDITION_ENUMERATION_ALTERED:
                case SERVICE_CONDITION_ENUMERATION_ARRIVES_EARLY:
                case SERVICE_CONDITION_ENUMERATION_REPLACEMENT_TRANSPORT:
                case SERVICE_CONDITION_ENUMERATION_SPLITTING_TRAIN:
                    return Effect.MODIFIED_SERVICE;

                case SERVICE_CONDITION_ENUMERATION_ON_TIME:
                case SERVICE_CONDITION_ENUMERATION_FULL_LENGTH_SERVICE:
                case SERVICE_CONDITION_ENUMERATION_NORMAL_SERVICE:
                    return Effect.OTHER_EFFECT;

                case SERVICE_CONDITION_ENUMERATION_UNDEFINED_SERVICE_INFORMATION:
                case SERVICE_CONDITION_ENUMERATION_UNKNOWN:
                    return Effect.UNKNOWN_EFFECT;

            }

            _log.warn("unknown condition: " + conditions);
            return Effect.UNKNOWN_EFFECT;
        }
        return null;
    }
}
