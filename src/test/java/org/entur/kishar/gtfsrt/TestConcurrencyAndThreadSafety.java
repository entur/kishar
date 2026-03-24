package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.SiriRecord;
import org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Autowired
    private AlertFactory alertFactory;

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
     * Test AlertFactory is thread-safe.
     * Verifies that 20 concurrent threads can each call createAlertFromSituation without errors,
     * and that all results are non-null (confirming the Spring-managed bean is properly injected).
     */
    @Test
    public void alertFactoryIsThreadSafe() throws InterruptedException, ExecutionException {
        assertNotNull(alertFactory, "AlertFactory must be injected by Spring");

        PtSituationElementRecord situation = Helper.createPtSituationElement("TST");
        int threadCount = 20;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<GtfsRealtime.Alert.Builder>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await(); // wait for all threads to be ready
                    return alertFactory.createAlertFromSituation(situation);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("AlertFactory concurrency error: " + e.getMessage());
                    e.printStackTrace();
                    return null;
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        startLatch.countDown(); // release all threads simultaneously to maximise contention
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");
        assertEquals(0, errorCount.get(), "No exceptions should occur during concurrent AlertFactory calls");

        for (Future<GtfsRealtime.Alert.Builder> future : futures) {
            assertNotNull(future.get(), "createAlertFromSituation must return a non-null result");
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

    /**
     * Test getStatus() is consistent when called concurrently with setTripUpdates/setVehiclePositions/setAlerts.
     * Before the fix, the three volatile reads inside getStatus() were not atomic; a writer could
     * interleave between reads and produce an inconsistent snapshot.  After the fix, getStatus()
     * holds the readLock for the entire snapshot, so it must never throw and must always return
     * a parseable list string.
     */
    @Test
    public void testGetStatusIsConsistentUnderConcurrentWrites() throws InterruptedException {
        // Seed initial data so the volatile fields are non-empty FeedMessages
        Map<String, GtfsRealtime.FeedMessage> emptyByDatasource = new HashMap<>();
        GtfsRealtime.FeedMessage seedMsg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                        .setId("seed-1")
                        .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                        .setTripId("seed-trip-1")
                                        .build())
                                .build())
                        .build())
                .build();
        rtService.setTripUpdates(seedMsg, emptyByDatasource);
        rtService.setVehiclePositions(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), emptyByDatasource);
        rtService.setAlerts(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), emptyByDatasource);

        int writerCount = 3;
        int readerCount = 7;
        int totalThreads = writerCount + readerCount;
        int iterations = 200;

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        // Writer threads: repeatedly swap the feed messages
        for (int i = 0; i < writerCount; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterations; j++) {
                        GtfsRealtime.FeedMessage msg = GtfsRealtimeLibrary.createFeedMessageBuilder()
                                .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                        .setId("w" + threadId + "-e" + j)
                                        .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                                .setTrip(GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId("trip-" + j)
                                                        .build())
                                                .build())
                                        .build())
                                .build();
                        rtService.setTripUpdates(msg, emptyByDatasource);
                        rtService.setVehiclePositions(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), emptyByDatasource);
                        rtService.setAlerts(GtfsRealtimeLibrary.createFeedMessageBuilder().build(), emptyByDatasource);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("getStatus writer error: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
                return null;
            }));
        }

        // Reader threads: repeatedly call getStatus() and validate its format
        for (int i = 0; i < readerCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterations; j++) {
                        String status = rtService.getStatus();
                        assertNotNull(status, "getStatus() must not return null");
                        assertTrue(status.startsWith("[") && status.endsWith("]"),
                                "getStatus() must return a list string, got: " + status);
                        assertTrue(status.contains("tripUpdates:"),
                                "getStatus() must contain tripUpdates field, got: " + status);
                        assertTrue(status.contains("vehiclePositions:"),
                                "getStatus() must contain vehiclePositions field, got: " + status);
                        assertTrue(status.contains("alerts:"),
                                "getStatus() must contain alerts field, got: " + status);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("getStatus reader error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
                return null;
            }));
        }

        startLatch.countDown(); // release all threads simultaneously
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All getStatus concurrency threads should complete within timeout");
        assertEquals(0, errorCount.get(), "No errors should occur when calling getStatus() concurrently with writes");

        for (Future<?> future : futures) {
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("getStatus concurrency future should complete successfully: " + e.getMessage());
            }
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    }

    /**
     * Test GtfsRtMapper date formatting is thread-safe.
     * Verifies that concurrent calls to convertSiriToGtfsRt produce correct startDate strings.
     * With SimpleDateFormat this would corrupt internal state and produce garbled dates.
     * With DateTimeFormatter (thread-safe) all results must be the expected UTC date string.
     */
    @Test
    void gtfsRtMapperDateFormattingIsThreadSafe() throws InterruptedException, ExecutionException {
        // Fixed departure time in UTC: 2024-12-20T08:00:00Z → startDate should be "20241220"
        ZonedDateTime departureTime = ZonedDateTime.parse("2024-12-20T08:00:00Z");
        String expectedStartDate = "20241220";
        String expectedStartTime = "08:00:00";

        String vmXml = "<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\" " +
                "xmlns:ns2=\"http://www.ifopt.org.uk/acsb\" xmlns:ns3=\"http://www.ifopt.org.uk/ifopt\" " +
                "xmlns:ns4=\"http://datex2.eu/schema/2_0RC1/2_0\">\n" +
                "    <ServiceDelivery>\n" +
                "        <ResponseTimestamp>2024-12-20T08:00:00Z</ResponseTimestamp>\n" +
                "        <ProducerRef>ENT</ProducerRef>\n" +
                "        <VehicleMonitoringDelivery version=\"2.0\">\n" +
                "            <ResponseTimestamp>2024-12-20T08:00:00Z</ResponseTimestamp>\n" +
                "            <VehicleActivity>\n" +
                "                <RecordedAtTime>2024-12-20T08:00:00Z</RecordedAtTime>\n" +
                "                <ValidUntilTime>2024-12-20T08:10:00Z</ValidUntilTime>\n" +
                "                <MonitoredVehicleJourney>\n" +
                "                    <LineRef>TST:Line:1234</LineRef>\n" +
                "                    <FramedVehicleJourneyRef>\n" +
                "                        <DataFrameRef>2024-12-20</DataFrameRef>\n" +
                "                        <DatedVehicleJourneyRef>TST:ServiceJourney:1234</DatedVehicleJourneyRef>\n" +
                "                    </FramedVehicleJourneyRef>\n" +
                "                    <OriginAimedDepartureTime>2024-12-20T08:00:00Z</OriginAimedDepartureTime>\n" +
                "                    <DataSource>TST</DataSource>\n" +
                "                    <VehicleLocation>\n" +
                "                        <Longitude>10.56</Longitude>\n" +
                "                        <Latitude>59.63</Latitude>\n" +
                "                    </VehicleLocation>\n" +
                "                    <VehicleRef>TST:Vehicle:1234</VehicleRef>\n" +
                "                </MonitoredVehicleJourney>\n" +
                "            </VehicleActivity>\n" +
                "        </VehicleMonitoringDelivery>\n" +
                "    </ServiceDelivery>\n" +
                "</Siri>";

        SiriRecord siriRecord = createSiriRecord(vmXml);
        assertNotNull(siriRecord, "SiriRecord must be created successfully");

        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<List<String>>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                List<String> startDates = new ArrayList<>();
                try {
                    startLatch.await(); // wait for all threads to be ready
                    for (int j = 0; j < 50; j++) {
                        var result = rtService.convertSiriToGtfsRt(siriRecord);
                        // Extract startDate from the resulting vehicle position trip descriptor
                        for (var entry : result.entrySet()) {
                            GtfsRealtime.FeedEntity entity = GtfsRealtime.FeedEntity.parseFrom(
                                    entry.getValue().getData()
                            );
                            if (entity.hasVehicle()) {
                                String startDate = entity.getVehicle().getTrip().getStartDate();
                                if (!startDate.isEmpty()) {
                                    startDates.add(startDate);
                                }
                                String startTime = entity.getVehicle().getTrip().getStartTime();
                                if (!startTime.isEmpty()) {
                                    startDates.add(startTime);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("GtfsRtMapper concurrency error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
                return startDates;
            }));
        }

        startLatch.countDown(); // release all threads simultaneously to maximise contention
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent date formatting");

        for (Future<List<String>> future : futures) {
            List<String> dates = future.get();
            for (String value : dates) {
                // values alternate between startDate ("20241220") and startTime ("08:00:00")
                assertTrue(
                        value.equals(expectedStartDate) || value.equals(expectedStartTime),
                        "Date/time value must not be garbled by concurrent formatting. Got: " + value
                );
            }
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    }
}
