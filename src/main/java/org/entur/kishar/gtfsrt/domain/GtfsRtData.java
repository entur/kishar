package org.entur.kishar.gtfsrt.domain;

import com.google.protobuf.Duration;

public class GtfsRtData {
    private byte[] data;
    private Duration timeToLive;

    public GtfsRtData(byte[] data, Duration timeToLive) {
        this.data = data;
        this.timeToLive = timeToLive;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Duration getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(Duration timeToLive) {
        this.timeToLive = timeToLive;
    }
}
