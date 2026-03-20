# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kishar is a Spring Boot microservice that converts SIRI (Service Interface for Real-time Information) transit data to GTFS-RT (General Transit Feed Specification - Realtime) format. It receives SIRI data from Google Pub/Sub topics (produced by Anshar) and exposes GTFS-RT feeds via REST endpoints.

**Technology Stack:**
- Java 17
- Spring Boot 3.5.8 with WebFlux
- Apache Camel 4.16.0 for message routing
- Google Protocol Buffers 4.33.2
- Google Cloud Pub/Sub
- Redis (optional, for distributed deployments)
- Maven build system

## Common Development Commands

**Build and Run:**
```bash
# Build the project
mvn clean install

# Run the application locally
mvn spring-boot:run

# Run tests
mvn test

# Run a specific test
mvn test -Dtest=SiriToGtfsRealtimeServiceTest

# Build Docker image (requires env vars: CONTAINER_REPO, CONTAINER_REGISTRY_USER, CONTAINER_REGISTRY_PASSWORD)
mvn jib:build
```

**Protocol Buffers:**
- The GTFS-RT protobuf schema is in `src/main/proto/gtfs-realtime.proto`
- It is automatically compiled during Maven build via the protobuf-maven-plugin
- To update the proto file: `curl https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto -o src/main/proto/gtfs-realtime.proto`

## Architecture

### Data Flow Pipeline

```
Google Pub/Sub Topics → Camel Routes → SIRI Parsing → GTFS-RT Conversion → In-Memory/Redis Storage → REST API
```

The service processes three types of SIRI data, each with its own flow:

1. **SIRI-ET (Estimated Timetables)** → GTFS-RT Trip Updates
2. **SIRI-VM (Vehicle Monitoring)** → GTFS-RT Vehicle Positions
3. **SIRI-SX (Situation Exchange)** → GTFS-RT Service Alerts

### Core Components

**Routes** (`org.entur.kishar.routes/`):
- `PubSubRoute.java` - Configures Camel routes for consuming messages from Pub/Sub topics
- `GtfsRtProviderRoute.java` - Exposes REST endpoints for serving GTFS-RT feeds
- `RestRouteBuilder.java` - Base REST DSL configuration shared by routes
- `LivenessRoute.java` - Health check and monitoring endpoints
- Routes use Apache Camel's DSL with `direct:` endpoints for internal pipeline composition

**Conversion Logic** (`org.entur.kishar.gtfsrt/`):
- `SiriToGtfsRealtimeService.java` - Main orchestrator for SIRI→GTFS-RT conversion
- `AlertFactory.java` - Converts SIRI-SX situation elements to GTFS-RT alerts
- `RedisService.java` - Handles persistence to Redis (when enabled)
- `mappers/GtfsRtMapper.java` - Maps SIRI Avro models to GTFS-RT format
- `mappers/AvroHelper.java` - Utility for extracting Java types from Avro records
- `helpers/SiriLibrary.java` - Utility functions for SIRI data manipulation
- `helpers/GtfsRealtimeLibrary.java` - Builder utilities for GTFS-RT Protocol Buffer objects
- `domain/FeedEntityBuilder.java` - Wraps protobuf FeedEntity.Builder with domain logic
- `domain/GtfsRtData.java` - Holds serialized feed bytes, performs feed serialization
- `TripAndVehicleKey.java` / `domain/CompositeKey.java` - Cache key types for indexing entities

**Key Architectural Patterns:**
- Event-driven architecture with Pub/Sub as message broker
- Service orchestration pattern (SiriToGtfsRealtimeService coordinates conversion)
- Factory pattern (AlertFactory for alerts)
- Builder pattern (Protocol Buffer builders throughout)
- In-memory caching with optional Redis backend for horizontal scaling

### REST API Endpoints

**GTFS-RT Feeds** (default port 8888):
- `GET /api/trip-updates` - Returns serialized GTFS-RT trip updates (from SIRI-ET)
- `GET /api/vehicle-positions` - Returns serialized GTFS-RT vehicle positions (from SIRI-VM)
- `GET /api/alerts` - Returns serialized GTFS-RT service alerts (from SIRI-SX)

**Debug/Monitoring:**
- `GET /internal/debug/status` - Shows current state of all feeds
- `GET /internal/debug/reset` - Clears internal state
- `GET /health/ready`, `/health/up`, `/health/healthy` - Health checks
- `GET /health/scrape` - Prometheus metrics export

### Configuration

Configuration is in `src/main/resources/application.properties`:

**Pub/Sub Topics:**
```properties
kishar.pubsub.enabled=true
kishar.pubsub.topic.et=google-pubsub://<project-id>:<topic-name>  # SIRI-ET
kishar.pubsub.topic.vm=google-pubsub://<project-id>:<topic-name>  # SIRI-VM
kishar.pubsub.topic.sx=google-pubsub://<project-id>:<topic-name>  # SIRI-SX
```

**Datasource Filtering:**
Each data type has a whitelist of allowed datasources (operators):
```properties
kishar.datasource.et.whitelist=RUT,BNR,ENT
kishar.datasource.vm.whitelist=RUT,ENT
kishar.datasource.sx.whitelist=ENT
```

**Vehicle Proximity Settings** (controls when a vehicle is considered "close to stop"):
```properties
kishar.settings.vm.close.to.stop.percentage=95  # % of journey completed
kishar.settings.vm.close.to.stop.distance=500    # meters to stop
```

**Redis Configuration:**
```properties
kishar.redis.enabled=false  # Enable for distributed deployments
kishar.redis.host=127.0.0.1
kishar.redis.port=6379
```

**Local Development with Pub/Sub Emulator:**
The default `application.properties` is pre-configured to use a local Pub/Sub emulator (no GCP credentials needed):
```properties
spring.cloud.gcp.pubsub.emulatorHost=localhost:8085
spring.cloud.gcp.pubsub.project-id=test
```
For real GCP, add `spring.cloud.gcp.pubsub.credentials.location=file:///path/to/gcp/credentials.json`.

### State Management

The service maintains stateful in-memory caches of the latest GTFS-RT messages:
- Vehicle positions have a 5-minute grace period before being considered stale
- Feed messages are periodically serialized (10-second intervals)
- Redis can be enabled for sharing state across multiple instances
- When modifying state management, consider both in-memory and Redis code paths

### Dependencies

**SIRI Data Model:**
- Uses `org.entur:siri-avro-model` (version 2.0.3) for SIRI data structures
- SIRI data arrives from Anshar in Avro format
- Test utilities use `org.entur:siri-avro-mapper` (version 4.0.2)

**Integration Testing:**
- Tests use Spring Boot Test framework with `@SpringBootTest`
- Camel test support via `camel-test-spring-junit5`
- REST API testing with RestAssured (version 5.5.6)
- Test data is built by constructing JAXB SIRI objects (e.g. `PtSituationElement`) and converting them to Avro via `Jaxb2AvroConverter` — see `Helper.java` in the test package for examples

### Working with Protocol Buffers

When modifying GTFS-RT output:
1. The schema is defined in `src/main/proto/gtfs-realtime.proto`
2. Java classes are auto-generated during `mvn compile`
3. Use builder pattern: `FeedEntity.newBuilder().setId(...).build()`
4. Helper utilities in `GtfsRealtimeLibrary.java` wrap common operations
5. Feed serialization happens in `GtfsRtData.java`

### GraphQL Integration

The service can query a Journey Planner via GraphQL:
- `helpers/graphql/JourneyPlannerGraphQLClient.java` - HTTP client
- `helpers/graphql/ServiceJourneyService.java` - Service journey data provider
- Used for enriching GTFS-RT data with additional journey information