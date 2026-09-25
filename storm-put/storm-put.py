#!/usr/bin/env python3
"""
storm-put.py — high-rate S3 PUT storm generator (persistent connections, SigV4).

Purpose: reproduce the "quota request storm" — a client that ignores 403 and
retries PUTs in a tight, parallel loop — with enough request rate to actually
stress RGW. Unlike a shell `s3cmd` loop (fork + fresh TLS per PUT, a few req/s),
this keeps one TLS connection open per worker thread and reuses it, so a laptop
can push hundreds-to-thousands of PUT/s at the gateways.

Requests are correctly **SigV4-signed** so they authenticate successfully and get
rejected at the QUOTA check (403 QuotaExceeded) — the same expensive code path
(TLS + auth + quota lookup + index stat) the real storm exercises. (Unsigned junk
would be rejected earlier/cheaper at auth and wouldn't reproduce the cost.)

No third-party deps — stdlib only (http.client, hmac, hashlib, ssl, threading).

Creds: reads an s3cmd-style config (access_key/secret_key/host_base) by default,
or pass --access-key/--secret-key/--endpoint explicitly.

Examples
--------
  # 64 workers against the LB, path-style, TLS verify off, report each second
  ./storm-put.py --config ~/.s3cfg.mytest --bucket test-repo \
      --workers 64

  # explicit creds, run 90s then stop
  ./storm-put.py --endpoint LB_HOST --port 443 --no-verify \
      --access-key AKID --secret-key SECRET --bucket test-repo \
      --workers 128 --duration 90

Stop with Ctrl-C. Prints per-second req/s and a status-code breakdown so you can
confirm the storm is landing (look for a high rate of 403s) regardless of what any
CPU meter says.
"""
import argparse
import hashlib
import hmac
import http.client
import ssl
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import quote

# --------------------------------------------------------------------------- #
# SigV4
# --------------------------------------------------------------------------- #
def _hmac(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret, datestamp, region, service):
    k = _hmac(("AWS4" + secret).encode("utf-8"), datestamp)
    k = _hmac(k, region)
    k = _hmac(k, service)
    return _hmac(k, "aws4_request")


def sign_put(access_key, secret_key, region, host, bucket, key, body):
    """Return the headers dict for a SigV4-signed path-style PUT."""
    service = "s3"
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = "/" + bucket + "/" + quote(key, safe="/")
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join([
        "PUT", canonical_uri, "", canonical_headers, signed_headers, payload_hash,
    ])
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signing_key = _signing_key(secret_key, datestamp, region, service)
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return canonical_uri, {
        "Host": host,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
        "Authorization": authorization,
        "Content-Length": str(len(body)),
    }


# --------------------------------------------------------------------------- #
# config parsing (s3cmd-style ini)
# --------------------------------------------------------------------------- #
def parse_s3cfg(path):
    out = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith(("#", "[", ";")):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip()
    return out


# --------------------------------------------------------------------------- #
# worker
# --------------------------------------------------------------------------- #
class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.codes = Counter()

    def record(self, code):
        with self.lock:
            self.total += 1
            self.codes[code] += 1

    def snapshot(self):
        with self.lock:
            return self.total, Counter(self.codes)


def worker(wid, args, ctx, stats, stop):
    host = args.endpoint
    body = b"x" * args.size
    conn = None
    n = 0
    backoff_streak = 0  # consecutive retryable rejections (for --backoff)
    while not stop.is_set():
        if conn is None:
            try:
                conn = http.client.HTTPSConnection(
                    args.endpoint, args.port, timeout=args.timeout, context=ctx
                )
            except Exception:
                stats.record("conn-err")
                time.sleep(0.05)
                continue
        key = f"storm/w{wid}-{n}"
        n += 1
        try:
            uri, headers = sign_put(
                args.access_key, args.secret_key, args.region,
                host, args.bucket, key, body,
            )
            conn.request("PUT", uri, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()  # drain so the connection can be reused (keep-alive)
            stats.record(resp.status)
            if wid == 0 and n == 1:
                # sanity: confirm we hit the QUOTA path (not SignatureDoesNotMatch)
                snippet = data.decode("utf-8", "replace")[:300].replace("\n", " ")
                print(f"# first response: HTTP {resp.status} :: {snippet}",
                      file=sys.stderr)
            # --backoff models a WELL-BEHAVED client: on a retryable rejection
            # (503 SlowDown) it sleeps with exponential backoff, reset on anything
            # else. This lets a rate limit actually reduce the incoming rate.
            if args.backoff and resp.status == 503:
                backoff_streak += 1
                delay = min(args.backoff_cap,
                            args.backoff * (2 ** (backoff_streak - 1)))
                time.sleep(delay)
            else:
                backoff_streak = 0
        except Exception:
            stats.record("exc")
            try:
                conn.close()
            except Exception:
                pass
            conn = None


def main():
    p = argparse.ArgumentParser(
        description="High-rate SigV4 S3 PUT storm generator (persistent conns).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--config", help="s3cmd-style config for creds/endpoint")
    p.add_argument("--endpoint", help="host/IP (overrides config host_base)")
    p.add_argument("--port", type=int, default=443)
    p.add_argument("--access-key")
    p.add_argument("--secret-key")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--bucket", required=True)
    p.add_argument("--workers", type=int, default=64, help="parallel threads")
    p.add_argument("--size", type=int, default=1, help="PUT body bytes (default 1)")
    p.add_argument("--duration", type=int, default=0,
                   help="seconds to run (0 = until Ctrl-C)")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--no-verify", action="store_true", help="skip TLS verify")
    p.add_argument("--backoff", type=float, default=0.0, metavar="BASE_SEC",
                   help="model a well-behaved client: on 503, sleep "
                        "BASE*2^(streak) sec (exponential backoff). 0=off (tight "
                        "retry, worst case). Try 0.1")
    p.add_argument("--backoff-cap", type=float, default=30.0,
                   help="max backoff sleep in seconds (default 30)")
    args = p.parse_args()

    cfg = parse_s3cfg(args.config) if args.config else {}
    args.access_key = args.access_key or cfg.get("access_key")
    args.secret_key = args.secret_key or cfg.get("secret_key")
    args.endpoint = args.endpoint or cfg.get("host_base", "").split(":")[0]
    if cfg.get("host_base") and ":" in cfg["host_base"] and args.port == 443:
        try:
            args.port = int(cfg["host_base"].split(":")[1])
        except ValueError:
            pass
    if not (args.access_key and args.secret_key and args.endpoint):
        sys.exit("ERROR: need access_key, secret_key, and endpoint "
                 "(via --config or explicit flags).")

    ctx = ssl.create_default_context()
    if args.no_verify or cfg.get("check_ssl_certificate", "").lower() == "false":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    print(f"# storm: {args.workers} workers -> https://{args.endpoint}:{args.port} "
          f"bucket={args.bucket} body={args.size}B verify={not args.no_verify}")
    print(f"# {'time':>8}{'req/s':>10}{'total':>12}   status breakdown (this second)")

    stats = Stats()
    stop = threading.Event()
    threads = [threading.Thread(target=worker, args=(i, args, ctx, stats, stop),
                                daemon=True) for i in range(args.workers)]
    for t in threads:
        t.start()

    start = time.time()
    prev_total = 0
    prev_codes = Counter()
    try:
        while True:
            time.sleep(1.0)
            total, codes = stats.snapshot()
            rate = total - prev_total
            delta = codes - prev_codes
            brk = " ".join(f"{k}={v}" for k, v in sorted(delta.items(),
                                                         key=lambda x: str(x[0])))
            print(f"  {time.strftime('%H:%M:%S'):>8}{rate:>10,}{total:>12,}   {brk}")
            prev_total, prev_codes = total, Counter(codes)
            if args.duration and (time.time() - start) >= args.duration:
                break
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        total, codes = stats.snapshot()
        elapsed = max(time.time() - start, 1e-9)
        print(f"\n# stopped. {total:,} requests in {elapsed:.1f}s "
              f"= {total/elapsed:,.0f} req/s avg")
        print(f"# status totals: "
              + " ".join(f"{k}={v}" for k, v in sorted(codes.items(),
                                                       key=lambda x: str(x[0]))))


if __name__ == "__main__":
    main()
