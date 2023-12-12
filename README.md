# Reactive Pulsar Client show case application

This application was originally written as a demonstration of [Reactive Pulsar](https://github.com/datastax/reactive-pulsar) which predates [Pulsar Reactive Client](https://github.com/apache/pulsar-client-reactive_) and [Spring for Apache Pulsar](https://github.com/spring-projects/spring-pulsar). It has been updated in 12/2023 to use the latest versions of Pulsar Reactive Client and Spring for Apache Pulsar. This code base uses Pulsar Reactive Client directly, without the special annotation support Spring for Apache Pulsar. 

## Prerequisites

### Pulsar standalone

Starting Pulsar standalone
```bash
docker run --name pulsar-standalone -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:3.1.1 /pulsar/bin/pulsar standalone -nss -nfw
```

Tailing logs
```bash
docker logs -f pulsar-standalone
```

Stopping
```bash
docker stop pulsar-standalone
```

Deleting container
```bash
docker rm pulsar-standalone
```

## Usage

### Running the application

```bash
./gradlew :bootRun
```

### Running the webhook application

```bash
./gradlew :webhook-target-app:bootRun
```

## Telemetry ingest demonstration

### Sending 1M telemetry entries with curl, all in one request

```bash
{ for i in {1..1000000}; do echo '{"n": "device'$i'/sensor1", "v": '$i'.123}'; done; } \
    | curl -X POST -T - -H "Content-Type: application/x-ndjson" localhost:8081/telemetry
```

### Sending 1M telemetry entries with curl, 1 message per request, with up to 50 parallel requests

Note: this requires gnu xargs (on MacOS: `brew install findutils`, use `gxargs` instead of `xargs`)
and gnu parallel (`brew install parallel`/`apt install moreutils`).

```bash
{ for i in {1..1000000}; do echo -ne 'curl -s -X POST -d '\''{"n": "device'$i'/sensor1", "v": '$i'.123}'\'' -H "Content-Type: application/x-ndjson" localhost:8081/telemetry''\0'; done; } \
  | xargs -0 -P 2 -n 100 parallel -j 25 --
```

### Sending 10000 telemetry entries with curl, 1 message per request (slow!)

```bash
for i in {1..10000}; do
  echo '{"n": "device'$i'/sensor1", "v": '$i'.123}' \
    | curl -X POST -T - -H "Content-Type: application/x-ndjson" localhost:8081/telemetry
done
```

### Sending 1M telemetry entries with k6

Requires [installing k6](https://k6.io/docs/getting-started/installation/).
```bash
cd k6
k6 run -u 100 -i 1000000 telemetry_ingest.js
```


## Telemetry processing demonstration

### Start pulsar-client to consume from telemetry_median

```
pulsar-client consume -n 0 -s sub telemetry_median
```

### Sending 1M telemetry entries (1000 devices, 1000 metrics) with curl, all in one request

```bash
{ for i in {1..1000}; do for j in {1..1000}; do echo '{"n": "device'$i'/sensor1", "v": '$j'.123}'; done; done; } \
    | curl -X POST -T - -H "Content-Type: application/x-ndjson" localhost:8081/telemetry
```


## SSE / Server Sent Events demonstration

SSE/Server Servet Events uses `text/event-stream` which is [defined in the HTML standard](https://html.spec.whatwg.org/#parsing-an-event-stream).

curl with `-N` parameter can be used to demonstrate SSE. 
The backend support passing last event id in `Last-Event-ID` header or `lastEventId` query parameter. In addition, there is 
a poll parameter which takes a boolean value expressed with true/1/yes/false/0/no. 
The `/firehost` path uses the `telemetry_ingest` topic as the source and `/firehost/median` uses `telemetry_median`. 

```bash
# use telemetry_ingest topic
curl -N localhost:8081/firehose/ingest
# use telemetry_median topic
curl -N localhost:8081/firehose/median
```

To demonstrate this, use one of the previous commands to send 1M telemetry entries with curl while the SSE curl command is running.

The SSE firehost example supports the `lastEventId` query parameter to continue from a specific message id. An application can use this for resuming message processing after a specific message id. 

The `poll` query parameter can be used to control whether the SSE connection should be kept open or closed after the last message has been sent. The default value for `poll` is `1` which means that the connection will be kept open. Setting `poll` to `0` will close the connection after the currently last message has been sent from the server to the client.
 
```bash
curl -N 'localhost:8081/firehose/median?lastEventId=CLoYEOEHIAAwAQ==&poll=0'
```