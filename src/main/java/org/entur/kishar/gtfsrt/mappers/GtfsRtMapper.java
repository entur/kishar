package org.entur.kishar.gtfsrt.mappers;

import com.google.transit.realtime.GtfsRealtime;
import uk.org.siri.siri20.*;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
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

    public GtfsRealtime.TripUpdate.Builder mapTripUpdateFromVehicleJourney(EstimatedVehicleJourney vehicleJourney) {
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

        VehicleActivityStructure.MonitoredVehicleJourney mvj = activity.getMonitoredVehicleJourney();
        if (mvj == null) {
            return null;
        }

        LocationStructure location = mvj.getVehicleLocation();

        if (location != null && location.getLatitude() != null
                && location.getLongitude() != null) {

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

            Instant time = activity.getRecordedAtTime().toInstant();
            if (time != null) {
                vp.setTimestamp(time.getEpochSecond());
            }

            GtfsRealtime.Position.Builder position = GtfsRealtime.Position.newBuilder();
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
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.STOPPED_AT);
                } else if (isCloseToNextStop) {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.INCOMING_AT);
                } else {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO);
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
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.FULL);
                        break;
                    case STANDING_AVAILABLE:
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.STANDING_ROOM_ONLY);
                        break;
                    case SEATS_AVAILABLE:
                        vp.setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.FEW_SEATS_AVAILABLE);
                        break;
                }
            }

            //Congestion
            if (mvj.isInCongestion() != null) {
                if (mvj.isInCongestion()) {
                    vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.CONGESTION);
                } else {
                    vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.RUNNING_SMOOTHLY);
                }
            }

            return vp;
        }

        return null;
    }

    private GtfsRealtime.VehicleDescriptor getMonitoredVehicleJourneyAsVehicleDescriptor(VehicleActivityStructure.MonitoredVehicleJourney mvj) {
        VehicleRef vehicleRef = mvj.getVehicleRef();
        if (vehicleRef == null || vehicleRef.getValue() == null) {
            return null;
        }

        GtfsRealtime.VehicleDescriptor.Builder vd = GtfsRealtime.VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

    private GtfsRealtime.TripDescriptor getMonitoredVehicleJourneyAsTripDescriptor(VehicleActivityStructure.MonitoredVehicleJourney mvj) {

        if (mvj.getFramedVehicleJourneyRef() == null) {
            return null;
        }

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();

        FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (mvj.getLineRef() != null) {
            td.setRouteId(mvj.getLineRef().getValue());
        }

        if (mvj.getOriginAimedDepartureTime() != null) {

            final Date date = new Date(mvj.getOriginAimedDepartureTime().toInstant().toEpochMilli());

            td.setStartDate(gtfsRtDateFormat.format(date));
            td.setStartTime(gtfsRtTimeFormat.format(date));
        }

        return td.build();
    }

    private GtfsRealtime.TripDescriptor getEstimatedVehicleJourneyAsTripDescriptor(EstimatedVehicleJourney estimatedVehicleJourney) {

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();
        FramedVehicleJourneyRefStructure fvjRef = estimatedVehicleJourney.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef());

        if (estimatedVehicleJourney.getLineRef() != null) {
            td.setRouteId(estimatedVehicleJourney.getLineRef().getValue());
        }

        return td.build();
    }

    private GtfsRealtime.VehicleDescriptor getEstimatedVehicleJourneyAsVehicleDescriptor(
            EstimatedVehicleJourney estimatedVehicleJourney) {
        VehicleRef vehicleRef = estimatedVehicleJourney.getVehicleRef();

        if (vehicleRef == null || vehicleRef.getValue() == null) {
            return null;
        }

        GtfsRealtime.VehicleDescriptor.Builder vd = GtfsRealtime.VehicleDescriptor.newBuilder();
        vd.setId(vehicleRef.getValue());
        return vd.build();
    }

    private void applyStopSpecificDelayToTripUpdateIfApplicable(
            EstimatedVehicleJourney mvj,
            GtfsRealtime.TripUpdate.Builder tripUpdate) {
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

    private Integer calculateDiff(ZonedDateTime aimed, ZonedDateTime expected) {
        if (aimed != null && expected != null) {
            return (int)(expected.toEpochSecond() - aimed.toEpochSecond());
        }
        return null;
    }

    private void addStopTimeUpdate(StopPointRef stopPointRef, Integer arrivalDelayInSeconds, Integer departureDelayInSeconds, int stopSequence, GtfsRealtime.TripUpdate.Builder tripUpdate) {


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
