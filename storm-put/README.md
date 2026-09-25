# storm-put

A high-rate S3 `PUT` storm generator for **RGW load / rejected-request testing**.

`storm-put.py` opens N persistent, keep-alive TLS connections and fires correctly
**SigV4-signed** PUTs as fast as it can. Because the requests are properly signed,
they *authenticate successfully* and are only rejected further downstream (e.g. at
a bucket **quota** check → `403 QuotaExceeded`, or at a **rate limit** → `503
SlowDown`). That makes it a faithful way to measure what a *rejected* request
actually costs a gateway — the SigV4 verification is paid upstream of the quota
and rate-limit checks, so a rejected request is **not** free.

Python 3, **stdlib only** (`http.client`, `hmac`, `hashlib`, `ssl`, `threading`) —
no `boto3`, no `pip install`.

> ⚠️ **Authorized testing only.** This is a load generator that can peg a gateway's
> CPU. Run it only against **test clusters / endpoints you own or are authorized to
> test**. It is a diagnostic/repro tool, not an attack tool.

## Why not just loop `s3cmd`?

A shell `s3cmd` loop forks a fresh process and does a new TLS handshake per PUT, so
each worker manages only a few requests/sec — the overhead lands on the client, not
the gateway. `storm-put.py` reuses one connection per thread, so a laptop can push
**hundreds-to-thousands of PUT/s** at the gateways, which is what it takes to
actually reproduce a gateway CPU spike.

## Usage

```bash
# 64 workers against a load balancer, path-style, TLS verify off, report each second
./storm-put.py --config ~/.s3cfg.mytest --bucket test-repo --workers 64 --no-verify

# explicit creds, run 90s then stop
./storm-put.py --endpoint LB_HOST --port 443 --no-verify \
    --access-key AKID --secret-key SECRET --bucket test-repo \
    --workers 128 --duration 90
```

Stop with Ctrl-C. It prints per-second req/s and a live HTTP status-code breakdown,
so you can confirm the storm is landing (look for a high, steady rate of 403/503)
regardless of what any CPU meter says. The **first** response body is echoed to
stderr so you can confirm *which* 403 you hit (`QuotaExceeded` — the expensive
quota path — vs `SignatureDoesNotMatch`, which would be a cheap auth reject and
would under-represent the cost).

### Credentials

Reads an **s3cmd-style config** (`access_key` / `secret_key` / `host_base`) via
`--config`, or takes `--access-key` / `--secret-key` / `--endpoint` explicitly.
`check_ssl_certificate=False` in the config is honored (same as `--no-verify`).
No credentials are stored in the tool.

### Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--config` | – | s3cmd-style config for creds/endpoint |
| `--endpoint` / `--port` | – / 443 | host/IP (overrides config `host_base`) |
| `--access-key` / `--secret-key` | from config | explicit creds |
| `--region` | `us-east-1` | SigV4 region (RGW ignores the value but it must sign) |
| `--bucket` | *(required)* | target bucket |
| `--workers` | 64 | parallel threads (each = one keep-alive connection) |
| `--size` | 1 | PUT body size in bytes (small = measure request/auth cost) |
| `--duration` | 0 | seconds to run (0 = until Ctrl-C) |
| `--timeout` | 10 | per-connection socket timeout |
| `--no-verify` | off | skip TLS certificate verification |
| `--backoff BASE` | 0 (off) | model a **well-behaved client**: on a `503`, sleep `BASE * 2^streak` (exponential backoff), reset on any non-503. `0` = tight retry (worst case). Try `0.1`. |
| `--backoff-cap` | 30 | max backoff sleep, seconds |

## Modeling client behavior with `--backoff`

`--backoff` is the key knob for understanding whether a **rate limit** will help:

- **`--backoff 0` (tight retry)** models a client that ignores throttling. The
  incoming request rate — and gateway CPU — stays constant whether the gateway
  answers `403` or `503`. A rate limit changes the error code and protects
  downstream (RADOS / bucket index) but does **not** bound gateway CPU here.
- **`--backoff 0.1` (well-behaved)** models a client that backs off on `503`. The
  request rate collapses toward the accepted ceiling and gateway CPU drops toward
  idle — i.e. a rate limit bounds CPU **only because the client self-throttles**.

Pair it with a per-interval CPU sampler on the gateway hosts to see the effect.
```
