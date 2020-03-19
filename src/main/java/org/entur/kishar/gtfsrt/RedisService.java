package org.entur.kishar.gtfsrt;

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

    RedissonClient redisson;

    public RedisService() {

        Config config = new Config();
        config.useReplicatedServers()
                .addNodeAddress("redis://127.0.0.1:6379");

        redisson = Redisson.create(config);
    }

    public void writeVehiclePositions(List<GtfsRealtime.FeedEntity> vehiclePositions, String datasource) {
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

    public Map<byte[], byte[]> readAllVehiclePositions() {
        RLocalCachedMap<byte[], byte[]> vehiclePositionMap = redisson.getLocalCachedMap(VEHICLE_POSITION_MAP, ByteArrayCodec.INSTANCE, LocalCachedMapOptions.defaults());

        Set<byte[]> keys = vehiclePositionMap.keySet();
        Map<byte[], byte[]> result = vehiclePositionMap.getAll(keys);

        vehiclePositionMap.destroy();
        return result;
    }
}
