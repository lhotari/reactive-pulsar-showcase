# Reactive Pulsar Client show case application

## Prerequisites

### Cloning reactive-pulsar

Running this application requires cloning https://github.com/lhotari/reactive-pulsar to the parent directory of this project.

```bash
cd ..
git clone https://github.com/lhotari/reactive-pulsar
```

### Pulsar standalone

Starting Pulsar standalone
```bash
docker run --name pulsar-standalone -d -p 8080:8080 -p 6650:6650 apachepulsar/pulsar:latest /pulsar/bin/pulsar standalone
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
./gradlew bootRun
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
curl -N localhost:8081/firehose
```

continuing after a specific message id: 
```bash
curl -N 'localhost:8081/firehose/median?lastEventId=CLoYEOEHIAAwAQ==&poll=0'
```