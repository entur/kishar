package org.entur.kishar.gtfsrt.mappers;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import uk.org.siri.www.siri.*;
import uk.org.siri.www.siri.FramedVehicleJourneyRefStructure;
import uk.org.siri.www.siri.LocationStructure;
import uk.org.siri.www.siri.MonitoredCallStructure;
import uk.org.siri.www.siri.ProgressBetweenStopsStructure;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class GtfsRtMapper {

    private final DateFormat gtfsRtDateFormat = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat gtfsRtTimeFormat = new SimpleDateFormat("HH:MM:ss");

    private int closeToNextStopPercentage;
    private int closeToNextStopDistance;

    public GtfsRtMapper(int closeToNextStopPercentage, int closeToNextStopDistance) {
        this.closeToNextStopPercentage = closeToNextStopPercentage;
        this.closeToNextStopDistance = closeToNextStopDistance;
    }

    public GtfsRealtime.TripUpdate.Builder mapTripUpdateFromVehicleJourney(EstimatedVehicleJourneyStructure vehicleJourney) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();

        GtfsRealtime.TripDescriptor td = getEstimatedVehicleJourneyAsTripDescriptor(vehicleJourney);
        tripUpdate.setTrip(td);

        GtfsRealtime.VehicleDescriptor vd = getEstimatedVehicleJourneyAsVehicleDescriptor(vehicleJourney);
        if (vd != null) {
            tripUpdate.setVehicle(vd);
        }

        applyStopSpecificDelayToTripUpdateIfApplicable(vehicleJourney, tripUpdate);
        return tripUpdate;
    }

    public GtfsRealtime.VehiclePosition.Builder convertSiriToGtfsRt(VehicleActivityStructure activity) {

        if (activity == null) {
            return null;
        }

        VehicleActivityStructure.MonitoredVehicleJourneyType mvj = activity.getMonitoredVehicleJourney();
        if (!activity.hasMonitoredVehicleJourney()) {
            return null;
        }

        LocationStructure location = mvj.getVehicleLocation();

        if (mvj.hasVehicleLocation()) {

            GtfsRealtime.VehiclePosition.Builder vp = GtfsRealtime.VehiclePosition.newBuilder();

            GtfsRealtime.TripDescriptor td = getMonitoredVehicleJourneyAsTripDescriptor(mvj);
            if (td == null) {
                return null;
            }
            vp.setTrip(td);

            GtfsRealtime.VehicleDescriptor vd = getMonitoredVehicleJourneyAsVehicleDescriptor(mvj);
            if (vd != null) {
                vp.setVehicle(vd);
            }

            if (activity.hasRecordedAtTime()) {
                Instant time = getInstant(activity.getRecordedAtTime());
                if (time != null) {
                    vp.setTimestamp(time.getEpochSecond());
                }
            }

            GtfsRealtime.Position.Builder position = GtfsRealtime.Position.newBuilder();
            position.setLatitude((float)location.getLatitude());
            position.setLongitude((float)location.getLongitude());

            position.setBearing(mvj.getBearing());


            // Speed - not included in profile
            position.setSpeed(mvj.getVelocity());


            //Distance traveled since last stop
            boolean isCloseToNextStop = false;
            if (activity.getProgressBetweenStops() != null) {
                final ProgressBetweenStopsStructure progressBetweenStops = activity.getProgressBetweenStops();

                isCloseToNextStop = progressBetweenStops.getPercentage() > closeToNextStopPercentage;

                final BigDecimal linkDistance = BigDecimal.valueOf(progressBetweenStops.getLinkDistance());

                if (linkDistance != null) {
                    final BigDecimal distanceTravelled = linkDistance.multiply(BigDecimal.valueOf(progressBetweenStops.getPercentage()).divide(BigDecimal.valueOf(100)));
                    position.setOdometer(distanceTravelled.doubleValue());

                    if (linkDistance.doubleValue() - distanceTravelled.doubleValue() < closeToNextStopDistance) {
                        isCloseToNextStop = true;
                    }
                }

            }


            // VehicleStatus
            if (mvj.hasMonitoredCall()) {
                final MonitoredCallStructure monitoredCall = mvj.getMonitoredCall();
                if (monitoredCall.getVehicleAtStop()) {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.STOPPED_AT);
                } else if (isCloseToNextStop) {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.INCOMING_AT);
                } else {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO);
                }

                final LocationStructure locationAtStop = monitoredCall.getVehicleLocationAtStop();
                if (monitoredCall.hasVehicleLocationAtStop()) {
                    position.setLatitude((float)locationAtStop.getLatitude());
                    position.setLongitude((float)locationAtStop.getLongitude());
                }

                if (monitoredCall.hasStopPointRef()) {
                    vp.setStopId(monitoredCall.getStopPointRef().getValue());
                }

                vp.setCurrentStopSequence(monitoredCall.getOrder());

            }

            vp.setPosition(position);

            //Occupancy - GTFS-RT experimental feature
            if (mvj.getOccupancy() != null) {
                switch (mvj.getOccupancy()) {
                    case OCCUPANCY_ENUMERATION_FULL:
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.FULL);
                        break;
                    case OCCUPANCY_ENUMERATION_STANDING_AVAILABLE:
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.STANDING_ROOM_ONLY);
                        break;
                    case OCCUPANCY_ENUMERATION_SEATS_AVAILABLE:
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.FEW_SEATS_AVAILABLE);
                        break;
                }
            }

            //Congestion
            if (mvj.getInCongestion()) {
                vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.CONGESTION);
            } else {
                vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.RUNNING_SMOOTHLY);
            }

            return vp;
        }

        return null;
    }

    private GtfsRealtime.VehicleDescriptor getMonitoredVehicleJourneyAsVehicleDescriptor(VehicleActivityStructure.MonitoredVehicleJourneyType mvj) {
        VehicleRefStructure vehicleRef = mvj.getVehicleRef();
        if (!mvj.hasVehicleRef() || vehicleRef.getValue() == null) {
            return null;
        }

        GtfsRealtime.VehicleDescriptor.Builder vd = GtfsRealtime.VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

    private GtfsRealtime.TripDescriptor getMonitoredVehicleJourneyAsTripDescriptor(VehicleActivityStructure.MonitoredVehicleJourneyType mvj) {

        if (!mvj.hasFramedVehicleJourneyRef()) {
            return null;
        }

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();

        FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (mvj.hasLineRef()) {
            td.setRouteId(mvj.getLineRef().getValue());
        }

        if (mvj.hasOriginAimedDepartureTime()) {

            final Date date = new Date(getInstant(mvj.getOriginAimedDepartureTime()).toEpochMilli());

            td.setStartDate(gtfsRtDateFormat.format(date));
            td.setStartTime(gtfsRtTimeFormat.format(date));
        }

        return td.build();
    }

    private Instant getInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private GtfsRealtime.TripDescriptor getEstimatedVehicleJourneyAsTripDescriptor(EstimatedVehicleJourneyStructure estimatedVehicleJourney) {

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();
        FramedVehicleJourneyRefStructure fvjRef = estimatedVehicleJourney.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (estimatedVehicleJourney.hasLineRef()) {
            td.setRouteId(estimatedVehicleJourney.getLineRef().getValue());
        }

        return td.build();
    }

    private GtfsRealtime.VehicleDescriptor getEstimatedVehicleJourneyAsVehicleDescriptor(
            EstimatedVehicleJourneyStructure estimatedVehicleJourney) {
        VehicleRefStructure vehicleRef = estimatedVehicleJourney.getVehicleRef();

        if (!estimatedVehicleJourney.hasVehicleRef() || vehicleRef.getValue() == null) {
            return null;
        }

        GtfsRealtime.VehicleDescriptor.Builder vd = GtfsRealtime.VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

    private void applyStopSpecificDelayToTripUpdateIfApplicable(
            EstimatedVehicleJourneyStructure mvj,
            GtfsRealtime.TripUpdate.Builder tripUpdate) {
        EstimatedVehicleJourneyStructure.EstimatedCallsType estimatedCalls = mvj.getEstimatedCalls();
        EstimatedVehicleJourneyStructure.RecordedCallsType recordedCalls = mvj.getRecordedCalls();

        int stopCounter = 0;
        if (mvj.hasRecordedCalls() && recordedCalls.getRecordedCallCount() > 0) {
            for (RecordedCallStructure recordedCall : recordedCalls.getRecordedCallList()) {
                StopPointRefStructure stopPointRef = recordedCall.getStopPointRef();
                if (!recordedCall.hasStopPointRef() || stopPointRef.getValue() == null) {
                    return;
                }
                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (recordedCall.hasAimedArrivalTime()) {
                    Timestamp updatedArrivalTime = null;
                    if (recordedCall.hasActualArrivalTime()) {
                        updatedArrivalTime = recordedCall.getActualArrivalTime();
                    } else if (recordedCall.hasExpectedArrivalTime()) {
                        updatedArrivalTime = recordedCall.getExpectedArrivalTime();
                    }
                    if (updatedArrivalTime != null) {
                        arrivalDelayInSeconds = calculateDiff(recordedCall.getAimedArrivalTime(), updatedArrivalTime);
                    }
                }

                if (recordedCall.hasAimedDepartureTime()) {

                    Timestamp updatedDepartureTime = null;
                    if (recordedCall.hasActualDepartureTime()) {
                        updatedDepartureTime = recordedCall.getActualDepartureTime();
                    } else if (recordedCall.hasExpectedDepartureTime()) {
                        updatedDepartureTime = recordedCall.getExpectedDepartureTime();
                    }

                    if (updatedDepartureTime != null) {
                        departureDelayInSeconds = calculateDiff(recordedCall.getAimedDepartureTime(), updatedDepartureTime);
                    }
                }

                int stopSequence;
                if (recordedCall.getOrder() > 0) {
                    stopSequence = recordedCall.getOrder() - 1;
                } else {
                    stopSequence = stopCounter;
                }

                addStopTimeUpdate(stopPointRef, arrivalDelayInSeconds, departureDelayInSeconds, stopSequence, tripUpdate);

                stopCounter++;
            }
        }
        if (mvj.hasEstimatedCalls() && estimatedCalls.getEstimatedCallCount() > 0) {
            for (EstimatedCallStructure estimatedCall : estimatedCalls.getEstimatedCallList()) {
                StopPointRefStructure stopPointRef = estimatedCall.getStopPointRef();
                if (!estimatedCall.hasStopPointRef() || stopPointRef.getValue() == null) {
                    return;
                }

                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (estimatedCall.hasAimedArrivalTime() && estimatedCall.hasExpectedArrivalTime()){
                    arrivalDelayInSeconds = calculateDiff(estimatedCall.getAimedArrivalTime(), estimatedCall.getExpectedArrivalTime());
                }
                if (estimatedCall.hasAimedDepartureTime() && estimatedCall.hasExpectedDepartureTime()) {
                    departureDelayInSeconds = calculateDiff(estimatedCall.getAimedDepartureTime(), estimatedCall.getExpectedDepartureTime());
                }

                int stopSequence;
                if (estimatedCall.getOrder() > 0) {
                    stopSequence = estimatedCall.getOrder() - 1;
                } else {
                    stopSequence = stopCounter;
                }

                addStopTimeUpdate(stopPointRef, arrivalDelayInSeconds, departureDelayInSeconds, stopSequence, tripUpdate);

                stopCounter++;
            }
        }
    }

    private Integer calculateDiff(Timestamp aimed, Timestamp expected) {
        if (aimed != null && expected != null) {
            return (int)Timestamps.between(aimed, expected).getSeconds();
        }
        return null;
    }

    private void addStopTimeUpdate(StopPointRefStructure stopPointRef, Integer arrivalDelayInSeconds, Integer departureDelayInSeconds, int stopSequence, GtfsRealtime.TripUpdate.Builder tripUpdate) {


        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        if (arrivalDelayInSeconds != null) {
            GtfsRealtime.TripUpdate.StopTimeEvent.Builder arrivalStopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
            arrivalStopTimeEvent.setDelay(arrivalDelayInSeconds);
            stopTimeUpdate.setArrival(arrivalStopTimeEvent);
        }
        if (departureDelayInSeconds != null) {
            GtfsRealtime.TripUpdate.StopTimeEvent.Builder departureStopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
            departureStopTimeEvent.setDelay(departureDelayInSeconds);
            stopTimeUpdate.setDeparture(departureStopTimeEvent);
        }

        stopTimeUpdate.setStopSequence(stopSequence);
        stopTimeUpdate.setStopId(stopPointRef.getValue());

        tripUpdate.addStopTimeUpdate(stopTimeUpdate);
    }
}
