package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.redisson.Redisson;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class RedisService {

    enum Type {
        VEHICLE_POSITION("vehiclePositionMap"),
        TRIP_UPDATE("tripUpdateMap"),
        ALERT("alertMap");

        private String mapIdentifier;

        Type(String mapIdentifier) {
            this.mapIdentifier = mapIdentifier;
        }

        public String getMapIdentifier() {
            return mapIdentifier;
        }
    }

    private static Logger LOG = LoggerFactory.getLogger(RedisService.class);
    private final boolean reddisEnabled;

    RedissonClient redisson;

    public RedisService(@Value("${kishar.redis.enabled:false}") boolean reddisEnabled, @Value("${kishar.redis.host:}") String host, @Value("${kishar.redis.port:}") String port) {
        this.reddisEnabled = reddisEnabled;

        if (reddisEnabled) {
            LOG.info("redis url = " + host + ":" + port);
            Config config = new Config();
            config.useReplicatedServers()
                    .addNodeAddress("redis://" + host + ":" + port);

            redisson = Redisson.create(config);
        }
    }

    public void writeGtfsRt(Map<byte[], GtfsRtData> gtfsRt, Type type) {
        if (reddisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);
            for (byte[] key : gtfsRt.keySet()) {
                GtfsRtData gtfsRtData = gtfsRt.get(key);
                long timeToLive = gtfsRtData.getTimeToLive().getSeconds();
                if (timeToLive > 0) {
                    gtfsRtMap.put(key, gtfsRtData.getData(), timeToLive, TimeUnit.SECONDS);
                }
            }
        }
    }

    public Map<byte[], byte[]> readGtfsRtMap(Type type) {
        if (reddisEnabled) {
            RLocalCachedMap<byte[], byte[]> gtfsRtMap = redisson.getLocalCachedMap(type.getMapIdentifier(), ByteArrayCodec.INSTANCE, LocalCachedMapOptions.defaults());

            Set<byte[]> keys = gtfsRtMap.keySet();
            Map<byte[], byte[]> result = gtfsRtMap.getAll(keys);

            return result;
        } else {
            return Maps.newHashMap();
        }
    }
}
