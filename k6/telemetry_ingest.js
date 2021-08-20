import http from "k6/http";

export default function () {
    let data = {n: `device${__VU}${__ITER}/sensor1`, v: __ITER + 0.123}
    http.post("http://localhost:8081/telemetry", JSON.stringify(data),
        {headers: {'Content-Type': 'application/x-ndjson'}});
};