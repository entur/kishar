package org.entur.kishar.gtfsrt.helpers;

import com.google.transit.realtime.GtfsRealtime;

public class GtfsRealtimeLibrary {
    public static GtfsRealtime.FeedMessage.Builder createFeedMessageBuilder() {
        long now = System.currentTimeMillis();
        com.google.transit.realtime.GtfsRealtime.FeedHeader.Builder header = GtfsRealtime.FeedHeader.newBuilder();
        header.setTimestamp(now / 1000L);
        header.setIncrementality(GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET);
        header.setGtfsRealtimeVersion("1.0");
        GtfsRealtime.FeedMessage.Builder feedMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        feedMessageBuilder.setHeader(header);
        return feedMessageBuilder;
    }
}
