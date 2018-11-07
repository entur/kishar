# kishar [![CircleCI](https://circleci.com/gh/entur/kishar/tree/master.svg?style=svg)](https://circleci.com/gh/entur/kishar/tree/master)

The Primordial god Kishar is the wife - and sister - of Anshar.

Fetches/imports SIRI-data from Anshar and converts it to GTFS-RT.

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
