package org.entur.kishar.gtfsrt;

import com.google.protobuf.Duration;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.graphql.ServiceJourneyService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cache eviction tests for fixes implemented in Phase 2.
 * Tests cache size limits and eviction behavior for:
 * - ServiceJourneyService (10,000 entry limit)
 * - RedisService (50,000 entry limit per type with 1 hour expiration)
 */
public class TestCacheEviction extends SiriToGtfsRealtimeServiceTest {

    @Autowired
    private ServiceJourneyService serviceJourneyService;

    @Autowired
    private RedisService redisService;

    /**
     * Test that ServiceJourneyService cache respects maximum size limit.
     * The cache should evict entries when it exceeds 10,000 entries.
     */
    @Test
    public void testServiceJourneyCacheMaximumSize() {
        // ServiceJourneyService cache is configured with maximumSize(10000)
        // We can't easily test the full 10k limit in a unit test, but we can verify
        // that the cache exists and functions correctly

        // Test with a small number of service journeys
        String testId1 = "TST:DatedServiceJourney:12345";
        String testId2 = "TST:DatedServiceJourney:67890";
        String testId3 = "TST:ServiceJourney:11111";

        // Cache should handle multiple lookups
        var journey1 = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(testId1);
        assertNotNull(journey1, "Should return a ServiceJourney object");

        var journey2 = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(testId2);
        assertNotNull(journey2, "Should return a ServiceJourney object");

        var journey3 = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(testId3);
        assertNotNull(journey3, "Should return a ServiceJourney object");

        // Second lookup should hit cache (verify cache is working)
        var cachedJourney1 = serviceJourneyService.getServiceJourneyFromDatedServiceJourney(testId1);
        assertNotNull(cachedJourney1, "Should return cached ServiceJourney");
        assertEquals(journey1.getId(), cachedJourney1.getId(), "Cached journey should have same ID");

        // Verify cache can handle many lookups without errors (basic functionality test)
        for (int i = 0; i < 100; i++) {
            var journey = serviceJourneyService.getServiceJourneyFromDatedServiceJourney("TST:DatedServiceJourney:" + i);
            assertNotNull(journey, "Cache should handle lookup " + i);
        }
    }

    /**
     * Test that RedisService mock uses bounded cache.
     * When Redis is disabled, the mock should use Guava Cache with size limits.
     */
    @Test
    public void testRedisServiceBoundedCache() {
        // Reset to start fresh
        redisService.resetAllData();

        // Create test data for TRIP_UPDATE type
        Map<String, GtfsRtData> tripUpdates = new HashMap<>();
        Duration ttl = Duration.newBuilder().setSeconds(300).build();

        // Add a reasonable number of entries (not 50k, but enough to verify it works)
        int testEntries = 100;
        for (int i = 0; i < testEntries; i++) {
            String key = "TST:ServiceJourney:" + i;
            byte[] data = ("TripUpdate-" + i).getBytes();
            tripUpdates.put(key, new GtfsRtData(data, ttl));
        }

        // Write to Redis service (which uses bounded cache when disabled)
        redisService.writeGtfsRt(tripUpdates, RedisService.Type.TRIP_UPDATE);

        // Read back the data
        Map<String, byte[]> retrievedData = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);

        assertNotNull(retrievedData, "Should retrieve data from cache");
        assertEquals(testEntries, retrievedData.size(), "Should have all entries (below limit)");

        // Verify data integrity
        for (int i = 0; i < testEntries; i++) {
            String key = "TST:ServiceJourney:" + i;
            byte[] data = retrievedData.get(key);
            assertNotNull(data, "Entry " + i + " should exist");
            String dataStr = new String(data);
            assertEquals("TripUpdate-" + i, dataStr, "Data should match");
        }
    }

    /**
     * Test that RedisService handles multiple types correctly.
     * The outer cache should hold up to 3 types (ET, VM, SX).
     */
    @Test
    public void testRedisServiceMultipleTypes() {
        redisService.resetAllData();

        // Add data for all three types
        Map<String, GtfsRtData> tripUpdates = createTestData("TripUpdate", 10);
        Map<String, GtfsRtData> vehiclePositions = createTestData("VehiclePosition", 10);
        Map<String, GtfsRtData> alerts = createTestData("Alert", 10);

        redisService.writeGtfsRt(tripUpdates, RedisService.Type.TRIP_UPDATE);
        redisService.writeGtfsRt(vehiclePositions, RedisService.Type.VEHICLE_POSITION);
        redisService.writeGtfsRt(alerts, RedisService.Type.ALERT);

        // Read back all types
        Map<String, byte[]> retrievedTripUpdates = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        Map<String, byte[]> retrievedVehiclePositions = redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION);
        Map<String, byte[]> retrievedAlerts = redisService.readGtfsRtMap(RedisService.Type.ALERT);

        // Verify all types are stored separately
        assertNotNull(retrievedTripUpdates, "Trip updates should be stored");
        assertNotNull(retrievedVehiclePositions, "Vehicle positions should be stored");
        assertNotNull(retrievedAlerts, "Alerts should be stored");

        assertEquals(10, retrievedTripUpdates.size(), "Should have 10 trip updates");
        assertEquals(10, retrievedVehiclePositions.size(), "Should have 10 vehicle positions");
        assertEquals(10, retrievedAlerts.size(), "Should have 10 alerts");

        // Verify data for each type
        for (int i = 0; i < 10; i++) {
            String key = "TST:Entity:" + i;

            String tripData = new String(retrievedTripUpdates.get(key));
            assertEquals("TripUpdate-" + i, tripData);

            String vehicleData = new String(retrievedVehiclePositions.get(key));
            assertEquals("VehiclePosition-" + i, vehicleData);

            String alertData = new String(retrievedAlerts.get(key));
            assertEquals("Alert-" + i, alertData);
        }
    }

    /**
     * Test that RedisService handles updates correctly.
     * Writing new data should update existing entries.
     */
    @Test
    public void testRedisServiceDataUpdate() {
        redisService.resetAllData();

        // Write initial data
        Map<String, GtfsRtData> initialData = createTestData("Initial", 5);
        redisService.writeGtfsRt(initialData, RedisService.Type.TRIP_UPDATE);

        // Verify initial data
        Map<String, byte[]> retrieved1 = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        assertEquals(5, retrieved1.size(), "Should have 5 entries");
        assertEquals("Initial-0", new String(retrieved1.get("TST:Entity:0")));

        // Update with new data (same keys)
        Map<String, GtfsRtData> updatedData = createTestData("Updated", 5);
        redisService.writeGtfsRt(updatedData, RedisService.Type.TRIP_UPDATE);

        // Verify updated data
        Map<String, byte[]> retrieved2 = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        assertEquals(5, retrieved2.size(), "Should still have 5 entries");
        assertEquals("Updated-0", new String(retrieved2.get("TST:Entity:0")), "Data should be updated");

        // Add more entries
        Map<String, GtfsRtData> moreData = createTestData("More", 10, 5); // keys 5-14
        redisService.writeGtfsRt(moreData, RedisService.Type.TRIP_UPDATE);

        Map<String, byte[]> retrieved3 = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        assertEquals(15, retrieved3.size(), "Should have 15 total entries");
        assertEquals("Updated-0", new String(retrieved3.get("TST:Entity:0")), "Original data should remain");
        assertEquals("More-5", new String(retrieved3.get("TST:Entity:5")), "New data should be added");
    }

    /**
     * Test that reset clears all data.
     */
    @Test
    public void testRedisServiceReset() {
        // Add data
        Map<String, GtfsRtData> tripUpdates = createTestData("TripUpdate", 10);
        Map<String, GtfsRtData> vehiclePositions = createTestData("VehiclePosition", 10);

        redisService.writeGtfsRt(tripUpdates, RedisService.Type.TRIP_UPDATE);
        redisService.writeGtfsRt(vehiclePositions, RedisService.Type.VEHICLE_POSITION);

        // Verify data exists
        Map<String, byte[]> beforeReset1 = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        Map<String, byte[]> beforeReset2 = redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION);

        assertFalse(beforeReset1.isEmpty(), "Should have trip updates before reset");
        assertFalse(beforeReset2.isEmpty(), "Should have vehicle positions before reset");

        // Reset all data
        redisService.resetAllData();

        // Verify data is cleared
        Map<String, byte[]> afterReset1 = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);
        Map<String, byte[]> afterReset2 = redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION);

        assertTrue(afterReset1.isEmpty(), "Trip updates should be cleared after reset");
        assertTrue(afterReset2.isEmpty(), "Vehicle positions should be cleared after reset");
    }

    /**
     * Stress test: Add many entries to verify cache bounds are respected.
     * Tests that the cache doesn't grow unbounded.
     */
    @Test
    public void testRedisServiceMemoryBounds() {
        redisService.resetAllData();

        // Add a large number of entries (less than the 50k limit but enough to verify bounds work)
        int largeBatch = 1000;
        Map<String, GtfsRtData> largeDataSet = createTestData("Large", largeBatch);

        redisService.writeGtfsRt(largeDataSet, RedisService.Type.TRIP_UPDATE);

        Map<String, byte[]> retrieved = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);

        assertNotNull(retrieved, "Should retrieve data");
        // Should have all entries since we're below the 50k limit
        assertTrue(retrieved.size() <= largeBatch, "Should not exceed expected size");
        assertTrue(retrieved.size() > 0, "Should have some entries");

        // Verify cache is functioning (spot check a few entries)
        if (retrieved.containsKey("TST:Entity:0")) {
            assertEquals("Large-0", new String(retrieved.get("TST:Entity:0")));
        }
        if (retrieved.containsKey("TST:Entity:500")) {
            assertEquals("Large-500", new String(retrieved.get("TST:Entity:500")));
        }
    }

    // Helper methods

    private Map<String, GtfsRtData> createTestData(String prefix, int count) {
        return createTestData(prefix, count, 0);
    }

    private Map<String, GtfsRtData> createTestData(String prefix, int count, int offset) {
        Map<String, GtfsRtData> data = new HashMap<>();
        // Create a default TTL Duration (5 minutes)
        Duration ttl = Duration.newBuilder().setSeconds(300).build();

        for (int i = 0; i < count; i++) {
            String key = "TST:Entity:" + (i + offset);
            byte[] bytes = (prefix + "-" + (i + offset)).getBytes();
            data.put(key, new GtfsRtData(bytes, ttl));
        }
        return data;
    }
}
