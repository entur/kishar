package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.VehiclePositionKey;
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

import java.io.IOException;
import java.util.List;
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

    private final static String VEHICLE_POSITION_MAP = "vehiclePositionMap";
    private final boolean reddisEnabled;

    RedissonClient redisson;

    public RedisService(@Value("${kishar.reddis.enabled}") boolean reddisEnabled, @Value("${kishar.reddis.url}") String url) {
        this.reddisEnabled = reddisEnabled;

        if (reddisEnabled) {
            Config config = new Config();
            config.useReplicatedServers()
                    .addNodeAddress("redis://" + url);

            redisson = Redisson.create(config);
        }
    }

    public void writeGtfsRt(Map<byte[], byte[]> gtfsRt, Type type) {
        if (reddisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);
            gtfsRtMap.putAll(gtfsRt, 5, TimeUnit.MINUTES);

            gtfsRtMap.destroy();
        }
    }

    public Map<byte[], byte[]> readGtfsRtMap(Type type) {
        if (reddisEnabled) {
            RLocalCachedMap<byte[], byte[]> gtfsRtMap = redisson.getLocalCachedMap(type.getMapIdentifier(), ByteArrayCodec.INSTANCE, LocalCachedMapOptions.defaults());

            Set<byte[]> keys = gtfsRtMap.keySet();
            Map<byte[], byte[]> result = gtfsRtMap.getAll(keys);

            gtfsRtMap.destroy();
            return result;
        } else {
            return Maps.newHashMap();
        }
    }
}
