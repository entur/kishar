package org.entur.kishar.gtfsrt.mappers;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.CallRecord;
import org.entur.avro.realtime.siri.model.EstimatedCallRecord;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.FramedVehicleJourneyRefRecord;
import org.entur.avro.realtime.siri.model.LocationRecord;
import org.entur.avro.realtime.siri.model.MonitoredVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.OccupancyEnum;
import org.entur.avro.realtime.siri.model.ProgressBetweenStopsRecord;
import org.entur.avro.realtime.siri.model.RecordedCallRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.entur.kishar.gtfsrt.helpers.graphql.model.ServiceJourney;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;

public class GtfsRtMapper extends AvroHelper {

    private final DateFormat gtfsRtDateFormat = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat gtfsRtTimeFormat = new SimpleDateFormat("HH:mm:ss");
    private final ServiceJourneyService serviceJourneyService;

    private final int closeToNextStopPercentage;
    private final int closeToNextStopDistance;

    public GtfsRtMapper(int closeToNextStopPercentage, int closeToNextStopDistance, ServiceJourneyService serviceJourneyService) {
        this.closeToNextStopPercentage = closeToNextStopPercentage;
        this.closeToNextStopDistance = closeToNextStopDistance;
        this.serviceJourneyService = serviceJourneyService;
    }

    public GtfsRealtime.TripUpdate.Builder mapTripUpdateFromVehicleJourney(EstimatedVehicleJourneyRecord vehicleJourney) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();

        GtfsRealtime.TripDescriptor td = getEstimatedVehicleJourneyAsTripDescriptor(vehicleJourney);
        if (td.getTripId() != null) {
            tripUpdate.setTrip(td);
        }

        GtfsRealtime.VehicleDescriptor vd = getEstimatedVehicleJourneyAsVehicleDescriptor(vehicleJourney);
        if (vd != null) {
            tripUpdate.setVehicle(vd);
        }

        applyStopSpecificDelayToTripUpdateIfApplicable(vehicleJourney, tripUpdate);
        return tripUpdate;
    }

    public GtfsRealtime.VehiclePosition.Builder convertSiriToGtfsRt(VehicleActivityRecord activity) {

        if (activity == null) {
            return null;
        }

        MonitoredVehicleJourneyRecord mvj = activity.getMonitoredVehicleJourney();
        if (mvj == null) {
            return null;
        }

        LocationRecord location = mvj.getVehicleLocation();

        if (location != null) {

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

            if (activity.getRecordedAtTime() != null) {
                vp.setTimestamp(
                        getInstant(activity.getRecordedAtTime())
                                .getEpochSecond()
                );
            }

            GtfsRealtime.Position.Builder position = GtfsRealtime.Position.newBuilder();
            position.setLatitude(location.getLatitude().floatValue());
            position.setLongitude(location.getLongitude().floatValue());

            if (mvj.getBearing() != null) {
                position.setBearing(mvj.getBearing());
            }

            if (mvj.getVelocity() != null) {
                // Speed - not included in profile
                position.setSpeed(mvj.getVelocity().floatValue());
            }


            //Distance traveled since last stop
            boolean isCloseToNextStop = false;
            ProgressBetweenStopsRecord progressBetweenStops = activity.getProgressBetweenStops();

            if (progressBetweenStops != null
                    && progressBetweenStops.getPercentage() != null
                    && progressBetweenStops.getLinkDistance() != null) {

                double linkDistance = progressBetweenStops.getLinkDistance();
                if (linkDistance > 0) {
                    double distanceTravelled = linkDistance * (progressBetweenStops.getPercentage() / 100);
                    position.setOdometer(distanceTravelled);

                    isCloseToNextStop = progressBetweenStops.getPercentage() > closeToNextStopPercentage
                            || (linkDistance - distanceTravelled < closeToNextStopDistance);
                } else {
                    isCloseToNextStop =  progressBetweenStops.getPercentage() > closeToNextStopPercentage;
                }
            }


            // VehicleStatus
            if (mvj.getMonitoredCall() != null) {
                final CallRecord monitoredCall = mvj.getMonitoredCall();
                if (Boolean.TRUE.equals(monitoredCall.getVehicleAtStop())) {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.STOPPED_AT);
                } else if (isCloseToNextStop) {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.INCOMING_AT);
                } else {
                    vp.setCurrentStatus(GtfsRealtime.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO);
                }

                final LocationRecord locationAtStop = monitoredCall.getVehicleLocationAtStop();
                if (monitoredCall.getVehicleLocationAtStop() != null) {
                    position.setLatitude(locationAtStop.getLatitude().floatValue());
                    position.setLongitude(locationAtStop.getLongitude().floatValue());
                }

                if (monitoredCall.getStopPointRef() != null) {
                    vp.setStopId(monitoredCall.getStopPointRef().toString());
                }

                if (monitoredCall.getOrder() != null) {
                    vp.setCurrentStopSequence(monitoredCall.getOrder());
                }

            }

            vp.setPosition(position);

            //Occupancy - GTFS-RT experimental feature
            if (mvj.getOccupancy() != null) {
                GtfsRealtime.VehiclePosition.OccupancyStatus gtfsRtOccupancy = convertOccupancy(mvj.getOccupancy().toString());
                if (gtfsRtOccupancy != null) {
                    vp.setOccupancyStatus(gtfsRtOccupancy);
                }
            }

            //Congestion
            if (Boolean.TRUE.equals(mvj.getInCongestion())) {
                vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.CONGESTION);
            } else {
                vp.setCongestionLevel(GtfsRealtime.VehiclePosition.CongestionLevel.RUNNING_SMOOTHLY);
            }

            return vp;
        }

        return null;
    }

    private static GtfsRealtime.VehiclePosition.OccupancyStatus convertOccupancy(String occupancy) {
        OccupancyEnum occupancyEnum = OccupancyEnum.valueOf(occupancy);
        return switch (occupancyEnum) {
            case EMPTY -> GtfsRealtime.VehiclePosition.OccupancyStatus.EMPTY;
            case FULL -> GtfsRealtime.VehiclePosition.OccupancyStatus.FULL;
            case STANDING_ROOM_ONLY, STANDING_AVAILABLE ->
                    GtfsRealtime.VehiclePosition.OccupancyStatus.STANDING_ROOM_ONLY;
            case CRUSHED_STANDING_ROOM_ONLY -> GtfsRealtime.VehiclePosition.OccupancyStatus.CRUSHED_STANDING_ROOM_ONLY;
            case SEATS_AVAILABLE, FEW_SEATS_AVAILABLE ->
                    GtfsRealtime.VehiclePosition.OccupancyStatus.FEW_SEATS_AVAILABLE;
            case NOT_ACCEPTING_PASSENGERS -> GtfsRealtime.VehiclePosition.OccupancyStatus.NOT_ACCEPTING_PASSENGERS;
            case MANY_SEATS_AVAILABLE -> GtfsRealtime.VehiclePosition.OccupancyStatus.MANY_SEATS_AVAILABLE;
            default -> null;
        };
    }

    private GtfsRealtime.VehicleDescriptor getMonitoredVehicleJourneyAsVehicleDescriptor(MonitoredVehicleJourneyRecord mvj) {
        String vehicleRef = mvj.getVehicleRef().toString();
        if (vehicleRef.isBlank()) {
            return null;
        }

        return GtfsRealtime.VehicleDescriptor.newBuilder()
                .setId(vehicleRef)
                .build();
    }

    private GtfsRealtime.TripDescriptor getMonitoredVehicleJourneyAsTripDescriptor(MonitoredVehicleJourneyRecord mvj) {

        if (mvj.getFramedVehicleJourneyRef() == null) {
            if (mvj.getVehicleJourneyRef() != null) {
                return getVehicleJourneyRefAsTripDescriptor(mvj.getVehicleJourneyRef(), mvj.getLineRef(), mvj.getOriginAimedDepartureTime());
            }
            return null;
        }

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();

        FramedVehicleJourneyRefRecord fvjRef = mvj.getFramedVehicleJourneyRef();
        td.setTripId(fvjRef.getDatedVehicleJourneyRef().toString());

        if (mvj.getLineRef() != null) {
            td.setRouteId( mvj.getLineRef().toString());
        }

        if (mvj.getOriginAimedDepartureTime() != null) {

            final Date date = new Date(getInstant(mvj.getOriginAimedDepartureTime()).toEpochMilli());

            td.setStartDate(gtfsRtDateFormat.format(date));
            td.setStartTime(gtfsRtTimeFormat.format(date));
        }

        return td.build();
    }

    private GtfsRealtime.TripDescriptor getVehicleJourneyRefAsTripDescriptor(CharSequence vehicleJourneyRef, CharSequence lineRef, CharSequence originAimedDepartureTime) {

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();

        ServiceJourney serviceJourney = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(vehicleJourneyRef.toString());

        if (serviceJourney != null && serviceJourney.getId() != null) {
            td.setTripId(serviceJourney.getId());

            if (lineRef != null) {
                td.setRouteId(lineRef.toString());
            }

            if (originAimedDepartureTime != null) {

                final Date date = new Date(getInstant(originAimedDepartureTime).toEpochMilli());

                td.setStartDate(gtfsRtDateFormat.format(date));
                td.setStartTime(gtfsRtTimeFormat.format(date));
            } else if (serviceJourney.getDate() != null){
                td.setStartDate(serviceJourney.getDate().replaceAll("-", ""));
            }
            return td.build();
        }
        return null;
    }


    private GtfsRealtime.TripDescriptor getEstimatedVehicleJourneyAsTripDescriptor(EstimatedVehicleJourneyRecord estimatedVehicleJourney) {

        GtfsRealtime.TripDescriptor.Builder td = GtfsRealtime.TripDescriptor.newBuilder();
        if (estimatedVehicleJourney.getFramedVehicleJourneyRef() != null) {
            FramedVehicleJourneyRefRecord fvjRef = estimatedVehicleJourney.getFramedVehicleJourneyRef();
            td.setTripId(fvjRef.getDatedVehicleJourneyRef().toString());
        } else if (estimatedVehicleJourney.getDatedVehicleJourneyRef() != null) {
            ServiceJourney serviceJourney = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(estimatedVehicleJourney.getDatedVehicleJourneyRef().toString());
            if (serviceJourney != null && serviceJourney.getId() != null) {
                td.setTripId(serviceJourney.getId());
                td.setStartDate(serviceJourney.getDate());
            }
        }
        if (estimatedVehicleJourney.getLineRef() != null) {
            td.setRouteId(estimatedVehicleJourney.getLineRef().toString());
        }

        return td.build();
    }

    private GtfsRealtime.VehicleDescriptor getEstimatedVehicleJourneyAsVehicleDescriptor(
            EstimatedVehicleJourneyRecord estimatedVehicleJourney) {

        if (estimatedVehicleJourney.getVehicleRef() == null) {
            return null;
        }

        return GtfsRealtime.VehicleDescriptor.newBuilder()
                .setId(estimatedVehicleJourney.getVehicleRef().toString())
                .build();
    }

    private void applyStopSpecificDelayToTripUpdateIfApplicable(
            EstimatedVehicleJourneyRecord mvj,
            GtfsRealtime.TripUpdate.Builder tripUpdate) {
        List<EstimatedCallRecord> estimatedCalls = mvj.getEstimatedCalls();
        List<RecordedCallRecord> recordedCalls = mvj.getRecordedCalls();

        int stopCounter = 0;
        if (recordedCalls != null) {
            for (RecordedCallRecord recordedCall : recordedCalls) {
                if (recordedCall.getStopPointRef() == null) {
                    return;
                }
                String stopPointRef = recordedCall.getStopPointRef().toString();
                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (recordedCall.getAimedArrivalTime() != null) {
                    CharSequence updatedArrivalTime = null;
                    if (recordedCall.getActualArrivalTime() != null) {
                        updatedArrivalTime = recordedCall.getActualArrivalTime();
                    } else if (recordedCall.getExpectedArrivalTime() != null) {
                        updatedArrivalTime = recordedCall.getExpectedArrivalTime();
                    }
                    if (updatedArrivalTime != null) {
                        arrivalDelayInSeconds = calculateDelay(recordedCall.getAimedArrivalTime(), updatedArrivalTime);
                    }
                }

                if (recordedCall.getAimedDepartureTime() != null) {

                    CharSequence updatedDepartureTime = null;
                    if (recordedCall.getActualDepartureTime() != null) {
                        updatedDepartureTime = recordedCall.getActualDepartureTime();
                    } else if (recordedCall.getExpectedDepartureTime() != null) {
                        updatedDepartureTime = recordedCall.getExpectedDepartureTime();
                    }

                    if (updatedDepartureTime != null) {
                        departureDelayInSeconds = calculateDelay(recordedCall.getAimedDepartureTime(), updatedDepartureTime);
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
        if (estimatedCalls != null) {
            for (EstimatedCallRecord estimatedCall : estimatedCalls) {
                if (estimatedCall.getStopPointRef() == null) {
                    return;
                }
                String  stopPointRef = estimatedCall.getStopPointRef().toString();

                Integer arrivalDelayInSeconds = null;
                Integer departureDelayInSeconds = null;

                if (estimatedCall.getAimedArrivalTime() != null && estimatedCall.getExpectedArrivalTime() != null){
                    arrivalDelayInSeconds = calculateDelay(estimatedCall.getAimedArrivalTime(), estimatedCall.getExpectedArrivalTime());
                }
                if (estimatedCall.getAimedDepartureTime() != null &&
                        estimatedCall.getExpectedDepartureTime() != null) {
                    departureDelayInSeconds = calculateDelay(estimatedCall.getAimedDepartureTime(), estimatedCall.getExpectedDepartureTime());
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

    private Integer calculateDelay(CharSequence aimed, CharSequence expected) {
        if (aimed != null && expected != null) {
            return (int) Duration.between(getInstant(aimed), getInstant(expected)).getSeconds();
        }
        return null;
    }

    private void addStopTimeUpdate(String stopPointRef, Integer arrivalDelayInSeconds, Integer departureDelayInSeconds, int stopSequence, GtfsRealtime.TripUpdate.Builder tripUpdate) {


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
        stopTimeUpdate.setStopId(stopPointRef);

        tripUpdate.addStopTimeUpdate(stopTimeUpdate);
    }
}
