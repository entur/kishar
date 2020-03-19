package org.entur.kishar.gtfsrt.domain;

import com.google.transit.realtime.GtfsRealtime;

import java.io.Serializable;

public class FeedEntityBuilder implements Serializable {

    private GtfsRealtime.FeedEntity feedEntity;

    public FeedEntityBuilder(GtfsRealtime.FeedEntity feedEntity) {
        this.feedEntity = feedEntity;
    }

    public GtfsRealtime.FeedEntity getFeedEntity() {
        return feedEntity;
    }

    public void setFeedEntity(GtfsRealtime.FeedEntity feedEntity) {
        this.feedEntity = feedEntity;
    }
}
