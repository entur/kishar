package org.entur.kishar.gtfsrt;

import org.junit.BeforeClass;

public abstract class SiriToGtfsRealtimeServiceTest {
    static final int NEXT_STOP_PERCENTAGE = 90;
    static final int NEXT_STOP_DISTANCE = 500;
    static SiriToGtfsRealtimeService rtService;

    @BeforeClass
    public static void initStatic() {
        rtService = new SiriToGtfsRealtimeService(new AlertFactory(),
                null, null, null,
                NEXT_STOP_PERCENTAGE, NEXT_STOP_DISTANCE);
    }
}
