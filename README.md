# kishar [![CircleCI](https://circleci.com/gh/entur/kishar/tree/master.svg?style=svg)](https://circleci.com/gh/entur/kishar/tree/master)

The Primordial god Kishar is the wife - and sister - of Anshar.

Receives SIRI-data (in protobuf-format) from a Google Pubsub-topic and converts it to GTFS-RT. The data for this topic is produced by Anshar (https://github.com/entur/anshar).

Inspired, and partially copied from https://github.com/OneBusAway/onebusaway-gtfs-realtime-from-siri-cli

GTFS-RT endpoints:
```
http://<server>:<port>/api/alerts

http://<server>:<port>/api/vehicle-positions

http://<server>:<port>/api/trip-updates
```


Healthcheck:
```
http://<server>:<port>/health/ready

http://<server>:<port>/health/up
```

# How to start this application
This application receives its data from a Google Pubsub-topic - thus, to start, it is necessary to add configuration for this in application.properties

Connect to Pubsub:
```
# Required config:
spring.cloud.gcp.project-id=<GCP project id>
spring.cloud.gcp.pubsub.project-id=<GCP project id
spring.cloud.gcp.pubsub.credentials.location=file:///path/to/gcp/credentials.json
```

Configuration of which topics to subscribe to:
```
kishar.pubsub.topic.et=entur-google-pubsub://<topic-name-siri-et>
kishar.pubsub.topic.vm=entur-google-pubsub://<topic-name-siri-vm>
kishar.pubsub.topic.sx=entur-google-pubsub://<topic-name-siri-sx>
```