# Reactive Pulsar Client show case application

## Telemetry ingest demonstration

### Sending 1M telemetry entries with curl, all in one request

```bash
{ for i in {1..1000000}; do echo '{"n": "device'$i'", "v": '$i'.123}'; done; } \
    | curl -X POST -T - -H "Content-Type: application/x-ndjson" localhost:8081/telemetry
```

### Sending 1M telemetry entries with curl, 1 message per request, with up to 50 parallel requests

Note: this requires gnu xargs (on MacOS: `brew install findutils`, use `gxargs` instead of `xargs`)
and gnu parallel (`brew install parallel`/`apt install moreutils`).

```bash
{ for i in {1..1000000}; do echo -ne 'curl -s -X POST -d '\''{"n": "device'$i'", "v": '$i'.123}'\'' -H "Content-Type: application/x-ndjson" localhost:8081/telemetry''\0'; done; } \
  | xargs -0 -P 2 -n 100 parallel -j 25 --
```

### Sending 10000 telemetry entries with curl, 1 message per request (slow!)

```bash
for i in {1..10000}; do
  echo '{"n": "device'$i'", "v": '$i'.123}' \
    | curl -X POST -T - -H "Content-Type: application/x-ndjson" localhost:8081/telemetry
done
```
