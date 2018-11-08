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

import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.Alert.Cause;
import com.google.transit.realtime.GtfsRealtime.Alert.Effect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.*;

import java.util.List;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.translation;

@Service
public class AlertFactory {

    private static final Logger _log = LoggerFactory.getLogger(AlertFactory.class);
    
    public Alert.Builder createAlertFromSituation(
            PtSituationElement ptSituation) {

        Alert.Builder alert = Alert.newBuilder();

        handleDescriptions(ptSituation, alert);
        handleOtherFields(ptSituation, alert);
        handlReasons(ptSituation, alert);
        handleAffects(ptSituation, alert);
        handleConsequences(ptSituation, alert);

        return alert;
    }

    private void handleDescriptions(PtSituationElement ptSituation,
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

    private void handleOtherFields(PtSituationElement ptSituation,
                                   Alert.Builder serviceAlert) {

        HalfOpenTimestampOutputRangeStructure window = ptSituation.getPublicationWindow();
        if (window != null) {
            TimeRange.Builder range = TimeRange.newBuilder();
            if (window.getStartTime() != null) {
                range.setStart(window.getStartTime().toEpochSecond());
            }
            if (window.getEndTime() != null) {
                range.setEnd(window.getEndTime().toEpochSecond());
            }
            if (range.hasStart() || range.hasEnd()) {
                serviceAlert.addActivePeriod(range);
            }
        }
    }

    private void handlReasons(PtSituationElement ptSituation,
                              Alert.Builder serviceAlert) {

        Cause cause = getReasonAsCause(ptSituation);
        if (cause != null) {
            serviceAlert.setCause(cause);
        }
    }

    private Cause getReasonAsCause(PtSituationElement ptSituation) {
        if (ptSituation.getEnvironmentReason() != null) {
            return Cause.WEATHER;
        }
        if (ptSituation.getEquipmentReason() != null) {
            switch (ptSituation.getEquipmentReason()) {
                case CONSTRUCTION_WORK:
                    return Cause.CONSTRUCTION;
                case CLOSED_FOR_MAINTENANCE:
                case MAINTENANCE_WORK:
                case EMERGENCY_ENGINEERING_WORK:
                case LATE_FINISH_TO_ENGINEERING_WORK:
                case REPAIR_WORK:
                    return Cause.MAINTENANCE;
                default:
                    return Cause.TECHNICAL_PROBLEM;
            }
        }
        if (ptSituation.getPersonnelReason() != null) {
            switch (ptSituation.getPersonnelReason()) {
                case INDUSTRIAL_ACTION:
                case UNOFFICIAL_INDUSTRIAL_ACTION:
                    return Cause.STRIKE;
            }
            return Cause.OTHER_CAUSE;
        }
        /**
         * There are really so many possibilities here that it's tricky to translate
         * them all
         */
        if (ptSituation.getMiscellaneousReason() != null) {
            switch (ptSituation.getMiscellaneousReason()) {
                case ACCIDENT:
                case COLLISION:
                    return Cause.ACCIDENT;
                case DEMONSTRATION:
                case MARCH:
                    return Cause.DEMONSTRATION;
                case PERSON_ILL_ON_VEHICLE:
                case FATALITY:
                    return Cause.MEDICAL_EMERGENCY;
                case POLICE_REQUEST:
                case BOMB_ALERT:
                case CIVIL_EMERGENCY:
                case EMERGENCY_SERVICES:
                case EMERGENCY_SERVICES_CALL:
                    return Cause.POLICE_ACTIVITY;
            }
        }

        return null;
    }

    /****
     * Affects
     ****/

    private void handleAffects(PtSituationElement ptSituation,
                               Alert.Builder serviceAlert) {

        AffectsScopeStructure affectsStructure = ptSituation.getAffects();

        if (affectsStructure == null) {
            return;
        }

        AffectsScopeStructure.Operators operators = affectsStructure.getOperators();

        if (operators != null
                && !operators.getAffectedOperators().isEmpty()) {

            for (AffectedOperatorStructure operator : operators.getAffectedOperators()) {
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

        AffectsScopeStructure.StopPoints stopPoints = affectsStructure.getStopPoints();

        if (stopPoints != null
                && !stopPoints.getAffectedStopPoints().isEmpty()) {

            for (AffectedStopPointStructure stopPoint : stopPoints.getAffectedStopPoints()) {
                StopPointRef stopRef = stopPoint.getStopPointRef();
                if (stopRef == null || stopRef.getValue() == null) {
                    continue;
                }
                String stopId = (stopRef.getValue());
                EntitySelector.Builder selector = EntitySelector.newBuilder();
                selector.setStopId(stopId);
                serviceAlert.addInformedEntity(selector);
            }
        }

        AffectsScopeStructure.VehicleJourneys vjs = affectsStructure.getVehicleJourneys();
        if (vjs != null
                && !vjs.getAffectedVehicleJourneies().isEmpty()) {

            for (AffectedVehicleJourneyStructure vj : vjs.getAffectedVehicleJourneies()) {

                EntitySelector.Builder selector = EntitySelector.newBuilder();

                if (vj.getLineRef() != null) {
                    String routeId = (vj.getLineRef().getValue());
                    selector.setRouteId(routeId);
                }

                List<VehicleJourneyRef> tripRefs = vj.getVehicleJourneyReves();
                AffectedVehicleJourneyStructure.Calls stopRefs = vj.getCalls();

                boolean hasTripRefs = !tripRefs.isEmpty();
                boolean hasStopRefs = stopRefs != null && !stopRefs.getCalls().isEmpty();

                if (!(hasTripRefs || hasStopRefs)) {
                    if (selector.hasRouteId()) {
                        serviceAlert.addInformedEntity(selector);
                    }
                } else if (hasTripRefs && hasStopRefs) {
                    for (VehicleJourneyRef vjRef : vj.getVehicleJourneyReves()) {
                        String tripId = (vjRef.getValue());
                        TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
                        tripDescriptor.setTripId(tripId);
                        selector.setTrip(tripDescriptor);
                        for (AffectedCallStructure call : stopRefs.getCalls()) {
                            String stopId = (call.getStopPointRef().getValue());
                            selector.setStopId(stopId);
                            serviceAlert.addInformedEntity(selector);
                        }
                    }
                } else if (hasTripRefs) {
                    for (VehicleJourneyRef vjRef : vj.getVehicleJourneyReves()) {
                        String tripId = (vjRef.getValue());
                        TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
                        tripDescriptor.setTripId(tripId);
                        selector.setTrip(tripDescriptor);
                        serviceAlert.addInformedEntity(selector);
                    }
                } else {
                    for (AffectedCallStructure call : stopRefs.getCalls()) {
                        String stopId = (call.getStopPointRef().getValue());
                        selector.setStopId(stopId);
                        serviceAlert.addInformedEntity(selector);
                    }
                }
            }
        }
    }

    private void handleConsequences(PtSituationElement ptSituation,
                                    Alert.Builder serviceAlert) {

        PtConsequencesStructure consequences = ptSituation.getConsequences();

        if (consequences == null || consequences.getConsequences() == null) {
            return;
        }

        for (PtConsequenceStructure consequence : consequences.getConsequences()) {
            if (consequence.getConditions() != null) {
                serviceAlert.setEffect(getConditionAsEffect(consequence.getConditions()));
            }
        }
    }

    private Effect getConditionAsEffect(List<ServiceConditionEnumeration> conditions) {
        for (ServiceConditionEnumeration condition : conditions) {

            switch (condition) {

                case CANCELLED:
                case NO_SERVICE:
                    return Effect.NO_SERVICE;

                case DELAYED:
                    return Effect.SIGNIFICANT_DELAYS;

                case DIVERTED:
                    return Effect.DETOUR;

                case ADDITIONAL_SERVICE:
                case EXTENDED_SERVICE:
                case SHUTTLE_SERVICE:
                case SPECIAL_SERVICE:
                case REPLACEMENT_SERVICE:
                    return Effect.ADDITIONAL_SERVICE;

                case DISRUPTED:
                case INTERMITTENT_SERVICE:
                case SHORT_FORMED_SERVICE:
                    return Effect.REDUCED_SERVICE;

                case ALTERED:
                case ARRIVES_EARLY:
                case REPLACEMENT_TRANSPORT:
                case SPLITTING_TRAIN:
                    return Effect.MODIFIED_SERVICE;

                case ON_TIME:
                case FULL_LENGTH_SERVICE:
                case NORMAL_SERVICE:
                    return Effect.OTHER_EFFECT;

                case UNDEFINED_SERVICE_INFORMATION:
                case UNKNOWN:
                    return Effect.UNKNOWN_EFFECT;

            }

            _log.warn("unknown condition: " + conditions);
            return Effect.UNKNOWN_EFFECT;
        }
        return null;
    }
}
