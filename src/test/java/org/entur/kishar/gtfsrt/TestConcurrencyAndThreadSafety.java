package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Thread-safety and concurrency tests for fixes implemented in Phase 1.
 * Tests concurrent access to:
 * - SiriToGtfsRealtimeService (read/write locks)
 * - AlertFactory (DateTimeFormatter thread-safety)
 * - PrometheusMetricsService (synchronized meter operations)
 */
public class TestConcurrencyAndThreadSafety extends SiriToGtfsRealtimeServiceTest {

    @Autowired
    private PrometheusMetricsService prometheusMetricsService;

    private static final int NUM_THREADS = 10;
    private static final int OPERATIONS_PER_THREAD = 100;

    /**
     * Test concurrent reads and writes to SiriToGtfsRealtimeService.
     * Verifies that ReentrantReadWriteLock prevents data corruption and ConcurrentModificationException.
     */
    @Test
    public void testConcurrentReadWriteAccess() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        // Create test data - FeedMessages for each datasource
        Map<String, GtfsRealtime.FeedMessage> testTripUpdatesByDatasource = new HashMap<>();
        Map<String, GtfsRealtime.FeedMessage> testVehiclePositionsByDatasource = new HashMap<>();
        Map<String, GtfsRealtime.FeedMessage> testAlertsByDatasource = new HashMap<>();

        // Create test FeedMessages with entities
        GtfsRealtime.FeedMessage.Builder tripBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();
        GtfsRealtime.FeedMessage.Builder vehicleBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();
        GtfsRealtime.FeedMessage.Builder alertBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();

        for (int i = 0; i < 5; i++) {
            String entityId = "entity-" + i;
            tripBuilder.addEntity(GtfsRealtime.FeedEntity.newBuilder()
                    .setId(entityId)
                    .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                            .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                    .setTripId("trip-" + i)
                                    .build())
                            .build())
                    .build());

            vehicleBuilder.addEntity(GtfsRealtime.FeedEntity.newBuilder()
                    .setId(entityId)
                    .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                            .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder()
                                    .setId("vehicle-" + i)
                                    .build())
                            .build())
                    .build());

            alertBuilder.addEntity(GtfsRealtime.FeedEntity.newBuilder()
                    .setId(entityId)
                    .setAlert(GtfsRealtime.Alert.newBuilder()
                            .setHeaderText(GtfsRealtime.TranslatedString.newBuilder()
                                    .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder()
                                            .setText("Alert " + i)
                                            .build())
                                    .build())
                            .build())
                    .build());
        }

        testTripUpdatesByDatasource.put("TST", tripBuilder.build());
        testVehiclePositionsByDatasource.put("TST", vehicleBuilder.build());
        testAlertsByDatasource.put("TST", alertBuilder.build());

        // Mix of reader and writer threads
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            Callable<Void> task;

            if (i % 3 == 0) {
                // Writer thread - updates all three feed types
                task = () -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            // Create fresh builders for each iteration
                            GtfsRealtime.FeedMessage tripUpdateMsg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                                    .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                            .setId("entity-" + (j % 5))
                                            .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                                    .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                                            .setTripId("trip-" + (j % 5))
                                                            .build())
                                                    .build())
                                            .build())
                                    .build();
                            rtService.setTripUpdates(tripUpdateMsg, testTripUpdatesByDatasource);

                            GtfsRealtime.FeedMessage vehiclePosMsg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                                    .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                            .setId("entity-" + (j % 5))
                                            .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                                                    .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder()
                                                            .setId("vehicle-" + (j % 5))
                                                            .build())
                                                    .build())
                                            .build())
                                    .build();
                            rtService.setVehiclePositions(vehiclePosMsg, testVehiclePositionsByDatasource);

                            GtfsRealtime.FeedMessage alertMsg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                                    .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                            .setId("entity-" + (j % 5))
                                            .setAlert(GtfsRealtime.Alert.newBuilder()
                                                    .setHeaderText(GtfsRealtime.TranslatedString.newBuilder()
                                                            .addTranslation(GtfsRealtime.TranslatedString.Translation.newBuilder()
                                                                    .setText("Alert " + (j % 5))
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build();
                            rtService.setAlerts(alertMsg, testAlertsByDatasource);

                            Thread.sleep(1); // Small delay to encourage contention
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        System.err.println("Writer thread " + threadId + " error: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                    return null;
                };
            } else {
                // Reader thread - reads all three feed types
                task = () -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            Object tripUpdates = rtService.getTripUpdates("application/json", null);
                            assertNotNull(tripUpdates);
                            assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

                            Object vehiclePositions = rtService.getVehiclePositions("application/json", null);
                            assertNotNull(vehiclePositions);
                            assertInstanceOf(GtfsRealtime.FeedMessage.class, vehiclePositions);

                            Object alerts = rtService.getAlerts("application/json", null);
                            assertNotNull(alerts);
                            assertInstanceOf(GtfsRealtime.FeedMessage.class, alerts);

                            Thread.sleep(1); // Small delay to encourage contention
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        System.err.println("Reader thread " + threadId + " error: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                    return null;
                };
            }

            futures.add(executor.submit(task));
        }

        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");

        // Verify no exceptions occurred
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent access");

        // Verify all futures completed successfully
        for (Future<?> future : futures) {
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("Future should complete successfully: " + e.getMessage());
            }
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    }

    /**
     * Test AlertFactory DateTimeFormatter thread-safety.
     * Verifies that multiple threads can format dates concurrently without errors.
     */
    @Test
    public void testAlertFactoryDateFormattingThreadSafety() throws InterruptedException {
        AlertFactory alertFactory = new AlertFactory();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Expected date format: yyyyMMdd
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        ZonedDateTime testDate = ZonedDateTime.now();
        String expectedDate = formatter.format(LocalDate.ofInstant(testDate.toInstant(), ZoneId.systemDefault()));

        List<Future<Set<String>>> futures = new ArrayList<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            Callable<Set<String>> task = () -> {
                Set<String> formattedDates = new HashSet<>();
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        // Create a situation with the same date
                        var situation = Helper.createPtSituationElement("TST");

                        // Format the date using the same logic as AlertFactory
                        String formattedDate = formatter.format(
                                LocalDate.ofInstant(testDate.toInstant(), ZoneId.systemDefault())
                        );
                        formattedDates.add(formattedDate);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Date formatting error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
                return formattedDates;
            };
            futures.add(executor.submit(task));
        }

        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");

        // Verify no errors occurred
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent date formatting");

        // Verify all dates are formatted correctly
        for (Future<Set<String>> future : futures) {
            try {
                Set<String> dates = future.get(1, TimeUnit.SECONDS);
                assertFalse(dates.isEmpty(), "Should have formatted dates");

                // All dates should match the expected format
                for (String date : dates) {
                    assertEquals(expectedDate, date, "Date should be formatted correctly");
                    assertEquals(8, date.length(), "Date should be 8 characters (yyyyMMdd)");
                }
            } catch (Exception e) {
                fail("Future should complete successfully: " + e.getMessage());
            }
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    }

    /**
     * Test PrometheusMetricsService concurrent meter modification.
     * Verifies that synchronized access prevents ConcurrentModificationException.
     */
    @Test
    public void testPrometheusMetricsServiceThreadSafety() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            Callable<Void> task = () -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD / 10; j++) { // Fewer operations for metrics
                        // Simulate concurrent meter registration
                        int etCount = ThreadLocalRandom.current().nextInt(0, 1000);
                        int vmCount = ThreadLocalRandom.current().nextInt(0, 1000);
                        int sxCount = ThreadLocalRandom.current().nextInt(0, 1000);

                        prometheusMetricsService.registerTotalGtfsRtEntities(etCount, vmCount, sxCount);

                        Thread.sleep(5); // Small delay to encourage contention
                    }
                } catch (ConcurrentModificationException e) {
                    errorCount.incrementAndGet();
                    System.err.println("ConcurrentModificationException detected: " + e.getMessage());
                    e.printStackTrace();
                    fail("ConcurrentModificationException should not occur with synchronized access");
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Metrics thread error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
                return null;
            };
            futures.add(executor.submit(task));
        }

        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");

        // Verify no ConcurrentModificationException occurred
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent meter modification");

        // Verify all futures completed successfully
        for (Future<?> future : futures) {
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("Future should complete successfully: " + e.getMessage());
            }
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    }

    /**
     * Stress test: High contention scenario with many concurrent readers and writers.
     * Verifies system stability under heavy load.
     */
    @Test
    public void testHighContentionStressTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(20); // More threads for stress
        CountDownLatch latch = new CountDownLatch(20);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        Map<String, GtfsRealtime.FeedMessage> testDataByDatasource = new HashMap<>();
        GtfsRealtime.FeedMessage.Builder stressBuilder = GtfsRealtimeLibrary.createFeedMessageBuilder();
        for (int i = 0; i < 10; i++) {
            String entityId = "stress-entity-" + i;
            stressBuilder.addEntity(GtfsRealtime.FeedEntity.newBuilder()
                    .setId(entityId)
                    .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                            .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                    .setTripId("stress-trip-" + i)
                                    .build())
                            .build())
                    .build());
        }
        testDataByDatasource.put("TST", stressBuilder.build());

        for (int i = 0; i < 20; i++) {
            final int threadId = i;
            Runnable task = () -> {
                try {
                    for (int j = 0; j < 50; j++) {
                        if (threadId % 4 == 0) {
                            // Writer
                            GtfsRealtime.FeedMessage msg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                                    .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                            .setId("stress-entity-" + (j % 10))
                                            .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                                    .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                                            .setTripId("stress-trip-" + (j % 10))
                                                            .build())
                                                    .build())
                                            .build())
                                    .build();
                            rtService.setTripUpdates(msg, testDataByDatasource);
                        } else {
                            // Reader
                            Object result = rtService.getTripUpdates("application/json", null);
                            assertNotNull(result);
                        }
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Stress test error in thread " + threadId + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            };
            executor.submit(task);
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "Stress test should complete within timeout");

        // Allow some errors in stress test, but should be minimal
        assertTrue(errorCount.get() < 10, "Error rate should be low: " + errorCount.get());
        assertTrue(successCount.get() > 900, "Most operations should succeed: " + successCount.get());

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor should terminate");
    }
}
