package org.entur.kishar.gtfsrt;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
@Configuration
public class RedisService {

    enum Type {
        VEHICLE_POSITION("vehiclePositionMap"),
        TRIP_UPDATE("tripUpdateMap"),
        ALERT("alertMap");

        private final String mapIdentifier;

        Type(String mapIdentifier) {
            this.mapIdentifier = mapIdentifier;
        }

        public String getMapIdentifier() {
            return mapIdentifier;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RedisService.class);
    private final boolean redisEnabled;

    private static Cache<String, Cache<String, byte[]>> hashMapRedisMock;

    RedissonClient redisson;

    public RedisService(@Value("${kishar.redis.enabled:false}") boolean redisEnabled,
                        @Value("${kishar.redis.host:}") String host,
                        @Value("${kishar.redis.port:}") String port,
                        @Value("${kishar.redis.password:}") String password) {
        this.redisEnabled = redisEnabled;

        if (redisEnabled) {
            LOG.info("redis url = " + host + ":" + port + ", password.length = " + (password == null ? 0 : password.length()));
            Config config = new Config();
            config.useReplicatedServers()
                    .addNodeAddress("redis://" + host + ":" + port)
                    .setPassword(password == null || password.isBlank() ? null : password);

            redisson = Redisson.create(config);
        } else {
            LOG.info("Redis not enabled - using bounded cache");
            hashMapRedisMock = CacheBuilder.newBuilder()
                    .maximumSize(3)  // 3 map types (ET, VM, SX)
                    .build();
        }
    }
    public void resetAllData() {
        if (redisEnabled) {
            LOG.info("Before - VEHICLE_POSITION: " + redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).size());
            redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).clear();
            LOG.info("After - VEHICLE_POSITION: " + redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).size());

            LOG.info("Before - TRIP_UPDATE: " + redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).size());
            redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).clear();
            LOG.info("After - TRIP_UPDATE: " + redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).size());

            LOG.info("Before - ALERT: " + redisson.getMap(Type.ALERT.mapIdentifier).size());
            redisson.getMap(Type.ALERT.mapIdentifier).clear();
            LOG.info("After - ALERT: " + redisson.getMap(Type.ALERT.mapIdentifier).size());
        }
        if (hashMapRedisMock != null) {
            hashMapRedisMock.invalidateAll();
        }
    }

    public void writeGtfsRt(Map<String, GtfsRtData> gtfsRt, Type type) {
        if (redisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);
            for (String key : gtfsRt.keySet()) {
                GtfsRtData gtfsRtData = gtfsRt.get(key);
                long timeToLive = gtfsRtData.getTimeToLive().getSeconds();
                if (timeToLive > 0) {
                    gtfsRtMap.put(key.getBytes(), gtfsRtData.getData(), timeToLive, TimeUnit.SECONDS);
                }
            }
        } else {
            try {
                Cache<String, byte[]> map = hashMapRedisMock.get(
                    type.getMapIdentifier(),
                    () -> CacheBuilder.newBuilder()
                        .maximumSize(50000)  // Max 50k entities per type (high traffic)
                        .expireAfterWrite(1, TimeUnit.HOURS)
                        .build()
                );
                for (String key : gtfsRt.keySet()) {
                    GtfsRtData gtfsRtData = gtfsRt.get(key);
                    map.put(key, gtfsRtData.getData());
                }
            } catch (Exception e) {
                LOG.error("Failed to write to cache", e);
            }
        }
    }

    public Map<String, byte[]> readGtfsRtMap(Type type) {
        if (redisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);

            Map<String, byte[]> result = new HashMap<>();

            final Set<Map.Entry<byte[], byte[]>> entries = gtfsRtMap.readAllEntrySet();
            for (Map.Entry<byte[], byte[]> entry : entries) {
                final CompositeKey key = CompositeKey.reCreate(entry.getKey());
                if (key != null) {
                    result.put(key.asString(), entry.getValue());
                }
            }

            return result;
        } else {
            Cache<String, byte[]> cache = hashMapRedisMock.getIfPresent(type.getMapIdentifier());
            return cache != null ? cache.asMap() : new HashMap<>();
        }
    }
}
