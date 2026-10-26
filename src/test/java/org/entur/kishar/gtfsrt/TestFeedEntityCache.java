package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.FeedEntityCache;
import org.entur.kishar.gtfsrt.domain.FeedEntityCache.CachedEntity;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestFeedEntityCache {

    @Test
    public void testUpdateReturnsParsedEntityWithDatasource() {
        FeedEntityCache cache = new FeedEntityCache();

        Collection<CachedEntity> result = cache.update(Map.of(key("1", "TST"), entity("1", "trip-1").toByteArray()));

        assertEquals(1, result.size());
        CachedEntity cached = result.iterator().next();
        assertEquals(entity("1", "trip-1"), cached.entity());
        assertEquals("TST", cached.datasource());
    }

    @Test
    public void testUnchangedEntityIsReused() {
        FeedEntityCache cache = new FeedEntityCache();
        FeedEntity first = single(cache.update(Map.of(key("1", "TST"), entity("1", "trip-1").toByteArray()))).entity();

        // Same bytes in a new array, as they arrive from Redis on every read
        FeedEntity second = single(cache.update(Map.of(key("1", "TST"), entity("1", "trip-1").toByteArray()))).entity();

        assertSame(first, second);
    }

    @Test
    public void testChangedEntityIsParsedAgain() {
        FeedEntityCache cache = new FeedEntityCache();
        FeedEntity first = single(cache.update(Map.of(key("1", "TST"), entity("1", "trip-1").toByteArray()))).entity();

        FeedEntity second = single(cache.update(Map.of(key("1", "TST"), entity("1", "trip-1-updated").toByteArray()))).entity();

        assertNotSame(first, second);
        assertEquals(entity("1", "trip-1-updated"), second);
    }

    @Test
    public void testEntityMissingFromLatestUpdateIsDropped() {
        FeedEntityCache cache = new FeedEntityCache();
        cache.update(Map.of(
                key("1", "TST"), entity("1", "trip-1").toByteArray(),
                key("2", "TST"), entity("2", "trip-2").toByteArray()));

        Collection<CachedEntity> result = cache.update(Map.of(key("2", "TST"), entity("2", "trip-2").toByteArray()));

        assertEquals(List.of(entity("2", "trip-2")), result.stream().map(CachedEntity::entity).toList());
    }

    @Test
    public void testEntriesWithInvalidKeyOrDataAreSkipped() {
        FeedEntityCache cache = new FeedEntityCache();
        Map<String, byte[]> entries = new HashMap<>();
        entries.put("not-a-composite-key", entity("1", "trip-1").toByteArray());
        entries.put(key("2", "TST"), "not-a-protobuf".getBytes(StandardCharsets.UTF_8));
        entries.put(key("3", "TST"), entity("3", "trip-3").toByteArray());

        Collection<CachedEntity> result = cache.update(entries);

        assertEquals(List.of(entity("3", "trip-3")), result.stream().map(CachedEntity::entity).toList());
    }

    private static CachedEntity single(Collection<CachedEntity> entities) {
        assertEquals(1, entities.size());
        return entities.iterator().next();
    }

    private static String key(String id, String datasource) {
        return new CompositeKey(id, datasource).asString();
    }

    private static FeedEntity entity(String id, String tripId) {
        FeedEntity.Builder builder = FeedEntity.newBuilder().setId(id);
        builder.getTripUpdateBuilder().getTripBuilder().setTripId(tripId);
        return builder.build();
    }
}
