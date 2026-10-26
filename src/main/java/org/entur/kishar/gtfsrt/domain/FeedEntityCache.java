package org.entur.kishar.gtfsrt.domain;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps the feed entities parsed in the previous feed build, so that entities whose bytes are unchanged in
 * Redis are reused instead of parsed again. Most entities are unchanged between two builds.
 */
public class FeedEntityCache {

    private static final Logger LOG = LoggerFactory.getLogger(FeedEntityCache.class);

    public record CachedEntity(byte[] data, FeedEntity entity, String datasource) {}

    private volatile Map<String, CachedEntity> entities = Map.of();

    /**
     * Returns the parsed entities for all valid entries, and replaces the cache with them. Entries with an
     * invalid key or invalid data are skipped.
     */
    public Collection<CachedEntity> update(Map<String, byte[]> entries) {
        Map<String, CachedEntity> previous = entities;
        Map<String, CachedEntity> current = new HashMap<>(entries.size() * 4 / 3 + 1);

        for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
            CachedEntity cached = previous.get(entry.getKey());
            if (cached == null || !Arrays.equals(cached.data(), entry.getValue())) {
                cached = parse(entry.getKey(), entry.getValue());
            }
            if (cached != null) {
                current.put(entry.getKey(), cached);
            }
        }

        entities = current;
        return current.values();
    }

    private static CachedEntity parse(String keyString, byte[] data) {
        CompositeKey key = CompositeKey.create(keyString);
        if (key == null) {
            return null;
        }
        try {
            return new CachedEntity(data, FeedEntity.parseFrom(data), key.getDatasource());
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Invalid feed entity from redis with key: {}", key, e);
            return null;
        }
    }
}
