package org.entur.kishar.gtfsrt;

import com.google.protobuf.Duration;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Feed output must reflect the current content of the shared store on every build, also when
 * parsed entities are reused between builds.
 */
public class TestFeedOutputAcrossBuilds extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testUpdatedEntityIsReflectedInNextBuild() {
        write("1", "TST", "trip-1");
        rtService.writeOutput();

        write("1", "TST", "trip-1-updated");
        rtService.writeOutput();

        assertEquals(List.of("trip-1-updated"), tripIds(rtService.getTripUpdates()));
    }

    @Test
    public void testUnchangedEntitiesStayInFeedAndByDatasource() {
        write("1", "TST", "trip-1");
        write("2", "BNR", "trip-2");
        rtService.writeOutput();
        rtService.writeOutput();

        assertEquals(List.of("trip-1", "trip-2"), tripIds(rtService.getTripUpdates()).stream().sorted().toList());
        assertEquals(List.of("trip-1"), tripIds(byDatasource("TST")));
        assertEquals(List.of("trip-2"), tripIds(byDatasource("BNR")));
    }

    @Test
    public void testRemovedEntitiesAreDroppedInNextBuild() {
        write("1", "TST", "trip-1");
        rtService.writeOutput();

        redisService.resetAllData();
        rtService.writeOutput();

        assertEquals(List.of(), tripIds(rtService.getTripUpdates()));
    }

    private void write(String id, String datasource, String tripId) {
        FeedEntity.Builder entity = FeedEntity.newBuilder().setId(id);
        entity.getTripUpdateBuilder().getTripBuilder().setTripId(tripId);
        redisService.writeGtfsRt(
                Map.of(new CompositeKey(id, datasource).asString(),
                        new GtfsRtData(entity.build().toByteArray(), Duration.newBuilder().setSeconds(3600).build())),
                RedisService.Type.TRIP_UPDATE);
    }

    private FeedMessage byDatasource(String datasource) {
        return (FeedMessage) rtService.getTripUpdates("application/json", datasource);
    }

    private static List<String> tripIds(FeedMessage feed) {
        return feed.getEntityList().stream().map(e -> e.getTripUpdate().getTrip().getTripId()).toList();
    }
}
