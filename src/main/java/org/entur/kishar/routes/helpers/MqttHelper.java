package org.entur.kishar.routes.helpers;

import com.google.transit.realtime.GtfsRealtime;

public class MqttHelper {

    private static final String SLASH = "/";
    private static final String TOPIC_PREFIX = "/gtfsrt/";

    public static String buildTopic(GtfsRealtime.VehiclePosition vp) {
        if (vp != null) {
            final String tripId = vp.getTrip().getTripId();
            final String lineId = vp.getTrip().getRouteId();
            final String startTime = vp.getTrip().getStartTime();
            final int directionId = vp.getTrip().getDirectionId();

            final String vehicleId = vp.getVehicle().getId();

            final String stopId = vp.getStopId();

            final GtfsRealtime.Position position = vp.getPosition();

            return getTopic(/*vehicleId,*/ lineId, tripId, directionId,/* headsign,*/ startTime, stopId, position);
        }
        return null;
    }

    /**
     * Formats topic to string
     * - /gtfsrt/<line>/<serviceJourney_id>/<direction>/<start_time>/<next_stop>/<geohash>;
     */
    private static String getTopic(/*String vehicleId,*/ String line, String tripId, int direction, //String headSign,
                            String startTime, String nextStop, GtfsRealtime.Position position) {
        return new StringBuilder(TOPIC_PREFIX)
//                .append(mode).append(SLASH)
//                .append(vehicleId).append(SLASH)
                .append(line).append(SLASH)
                .append(tripId).append(SLASH)
                .append(direction).append(SLASH)
//                .append(headSign).append(SLASH)
                .append(startTime).append(SLASH)
                .append(nextStop).append(SLASH)
                .append(getGeoHash(position.getLatitude(), position.getLongitude())).toString();
    }

    private static String getGeoHash(double latitude, double longitude) {
        StringBuilder geohash = new StringBuilder();
        geohash.append((int)latitude);
        geohash.append(";");
        geohash.append((int)longitude);
        geohash.append(digits(latitude, longitude));
        return geohash.toString();
    }

    private static String digits(double latitude, double longitude) {
        int j;
        StringBuilder results = new StringBuilder(SLASH);
        for (int i = j = 1; j <= 3; i = ++j) {
            results.append(digit(latitude, i)).append(digit(longitude, i)).append(SLASH);
        }
        return results.toString();
    }

    private static String digit(double x, int i) {
        return "" + (int) (Math.floor(x * Math.pow(10, i)) % 10);
    }

}
