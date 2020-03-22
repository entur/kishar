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
    private static Logger LOG = LoggerFactory.getLogger(RedisService.class);

    private final static String VEHICLE_POSITION_MAP = "vehiclePositionMap";
    private final boolean reddisEnabled;

    RedissonClient redisson;

    public RedisService(@Value("${kishar.reddis.enabled}") boolean reddisEnabled, @Value("${kishar.reddis.url}") String url) {
        LOG.info("Reddis URL: " + url);
        this.reddisEnabled = reddisEnabled;

        if (reddisEnabled) {
            Config config = new Config();
            config.useReplicatedServers()
                    .addNodeAddress("redis://" + url);

            redisson = Redisson.create(config);
        }
    }

    public void writeVehiclePositions(List<GtfsRealtime.FeedEntity> vehiclePositions, String datasource) {
        if (reddisEnabled) {
            RMapCache<byte[], byte[]> vehiclePositionMap = redisson.getMapCache(VEHICLE_POSITION_MAP, ByteArrayCodec.INSTANCE);
            for (GtfsRealtime.FeedEntity vehiclePosition : vehiclePositions) {

                try {
                    vehiclePositionMap.put(new VehiclePositionKey(vehiclePosition.getId(), datasource).toByteArray(), vehiclePosition.toByteArray(), 5, TimeUnit.MINUTES);
                } catch (IOException e) {
                    LOG.error("failed to serialize key to byte array", e);
                }
            }
            vehiclePositionMap.destroy();
        }
    }

    public Map<byte[], byte[]> readAllVehiclePositions() {
        if (reddisEnabled) {
            RLocalCachedMap<byte[], byte[]> vehiclePositionMap = redisson.getLocalCachedMap(VEHICLE_POSITION_MAP, ByteArrayCodec.INSTANCE, LocalCachedMapOptions.defaults());

            Set<byte[]> keys = vehiclePositionMap.keySet();
            Map<byte[], byte[]> result = vehiclePositionMap.getAll(keys);

            vehiclePositionMap.destroy();
            return result;
        } else {
            return Maps.newHashMap();
        }
    }
}
