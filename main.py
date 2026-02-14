"""
SpaceWatch – AI-Driven Observability Backend (Production Baseline, copy/paste)

Key goals this file satisfies:
- AI is the “driver”: user can ask anything; AI decides what tools to call.
- Backend sits between AI agent <-> DigitalOcean Spaces.
- Tools include: buckets, list objects, recent objects, storage summary, top largest,
  metrics snapshots/series/sources, list/read/search .log.gz access logs, object audit timeline.
- Backend NEVER returns tool-call JSON to the frontend; it executes tool calls internally.
- Robust parsing: extracts the FIRST JSON object from agent output, even if the model adds text.
- Enforces: ONE tool call per agent turn. If model returns multiple JSON objects, backend requests a retry.
- Safe limits: rate limiting, max tool steps, max bytes, max files/lines scanned.

How “last file uploaded” + “from which IP” works with this file:
1) AI calls recent_objects(bucket=...)
2) AI picks the most recent object key
3) AI calls object_audit(source_bucket=..., object_key=..., methods=["PUT"])
4) AI answers with the IP + timestamp

IMPORTANT: This app can only answer questions from:
- Spaces object listing (sizes/last_modified)
- Access logs you store in ACCESS_LOGS_BUCKET (supports .log and .log.gz)
- Snapshots you write (every 5 min) into METRICS_BUCKET/METRICS_PREFIX
It cannot magically provide AWS CloudWatch / Azure Monitor without additional integrations.

"""

import os
import re
import time
from collections import defaultdict
from typing import Optional
import json
import gzip
import io
import sys
import time
import traceback
import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple, Set, Callable

import httpx
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, SecretStr
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import logging

# Load environment variables from .env file
load_dotenv()

# ============================================================
# STRUCTURED LOGGING (AWS S3 / Azure Blob style)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("spacewatch")


# ============================================================
# ENV / CONFIG
# ============================================================
# Default region and endpoint - can be overridden per request
DEFAULT_SPACES_REGION = os.getenv("SPACES_REGION", "sgp1")
DEFAULT_SPACES_ENDPOINT = os.getenv("SPACES_ENDPOINT", f"https://{DEFAULT_SPACES_REGION}.digitaloceanspaces.com")

DO_AGENT_URL = os.getenv("DO_AGENT_URL")  # must be OpenAI-compatible chat completions endpoint
DO_AGENT_KEY = os.getenv("DO_AGENT_KEY")

# NOTE: ACCESS_LOGS_BUCKET and METRICS_BUCKET are no longer global defaults.
# Users must specify log_bucket and log_prefix per request.
METRICS_GZIP = os.getenv("METRICS_GZIP", "true").lower() in {"1", "true", "yes"}

# Optional: explicitly tell scheduler which source buckets to snapshot.
# If set, scheduler does NOT need list_buckets permission.
# Example: SCHEDULER_SOURCE_BUCKETS="prod-assets,prod-uploads"
SCHEDULER_SOURCE_BUCKETS = [b.strip() for b in os.getenv("SCHEDULER_SOURCE_BUCKETS", "").split(",") if b.strip()]


# Optional: protect endpoints with X-API-Key
APP_API_KEY = os.getenv("APP_API_KEY", "").strip()

# Optional fallback buckets if list_buckets() is disallowed
FALLBACK_BUCKETS = [b.strip() for b in os.getenv("FALLBACK_BUCKETS", "").split(",") if b.strip()]

# Safety limits
MAX_LOG_BYTES = int(os.getenv("MAX_LOG_BYTES", "10485760"))  # 10MB per object read
MAX_LOG_LINES = int(os.getenv("MAX_LOG_LINES", "300"))       # tail lines on read_log

MAX_LOG_FILES_SCAN = int(os.getenv("MAX_LOG_FILES_SCAN", "50"))
MAX_LOG_LINES_SCAN = int(os.getenv("MAX_LOG_LINES_SCAN", "50000"))

MAX_LIST_OBJECTS_RETURN = int(os.getenv("MAX_LIST_OBJECTS_RETURN", "200"))
MAX_TOP_LARGEST = int(os.getenv("MAX_TOP_LARGEST", "50"))

# Rate limiting
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "2.0"))
RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "10"))

# Bucket cache
BUCKET_CACHE_TTL_SEC = int(os.getenv("BUCKET_CACHE_TTL_SEC", "300"))

# Agent call config
DO_AGENT_TIMEOUT_SEC = float(os.getenv("DO_AGENT_TIMEOUT_SEC", "60"))
DO_AGENT_RETRIES = int(os.getenv("DO_AGENT_RETRIES", "2"))

# Agent tool loop
AGENT_MAX_STEPS = int(os.getenv("AGENT_MAX_STEPS", "8"))
AGENT_MAX_TOOL_BYTES = int(os.getenv("AGENT_MAX_TOOL_BYTES", "250000"))

# Optional scheduler to write snapshots every 5 minutes
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "false").lower() in {"1", "true", "yes"}
SNAPSHOT_EVERY_SEC = int(os.getenv("SNAPSHOT_EVERY_SEC", "300"))

# Optional leader gating if you run multiple replicas
SCHEDULER_INSTANCE_ID = os.getenv("SCHEDULER_INSTANCE_ID", "").strip()
SCHEDULER_LEADER_ID = os.getenv("SCHEDULER_LEADER_ID", "").strip()


if not all([DO_AGENT_URL, DO_AGENT_KEY]):
    print("\n" + "=" * 70)
    print("❌ CONFIGURATION ERROR")
    print("=" * 70)
    print("\nMissing required environment variables: DO_AGENT_URL, DO_AGENT_KEY")
    print("\nTo configure SpaceWatch, please run the interactive setup:")
    print("  python setup.py")
    print("\nOr manually create a .env file with the required variables.")
    print("See sample.env for an example configuration.")
    print("=" * 70 + "\n")
    raise RuntimeError("Missing required configuration")


# ============================================================
# DYNAMIC S3 CLIENT CREATION
# ============================================================
def create_s3_client(access_key: str, secret_key: str, region: Optional[str] = None, endpoint: Optional[str] = None):
    """
    Create an S3 client dynamically with provided user credentials.
    This allows multi-tenant usage where each request uses its own credentials.
    """
    if not access_key or not secret_key:
        raise HTTPException(status_code=400, detail="Spaces access key and secret key are required")
    
    region = region or DEFAULT_SPACES_REGION
    endpoint = endpoint or DEFAULT_SPACES_ENDPOINT
    
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def get_credential_cache_key(access_key: str, region: Optional[str] = None, endpoint: Optional[str] = None) -> str:
    """
    Generate a stable cache key for credential-scoped caching in BYOC scenarios.
    
    Hash the tuple (access_key_id, endpoint_url, region) to create a unique identifier
    for each set of credentials. This ensures that caches (buckets, IPs, metrics) are
    isolated per credential set, preventing cross-user data leakage.
    
    Note: We do NOT include the secret key in the cache key for security reasons.
    The access key alone is sufficient to identify unique credential sets.
    """
    region = region or DEFAULT_SPACES_REGION
    endpoint = endpoint or DEFAULT_SPACES_ENDPOINT
    
    # Create a stable string representation of the credential tuple
    credential_tuple = f"{access_key}:{endpoint}:{region}"
    
    # Hash it to create a stable, privacy-preserving cache key
    return hashlib.sha256(credential_tuple.encode()).hexdigest()[:16]


# ============================================================
# APP
# ============================================================
app = FastAPI(title="SpaceWatch – AI-driven Observability Backend (Prod Baseline)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# STORAGE OPERATION TRACKING MIDDLEWARE (S3/Azure Blob style)
# ============================================================
@app.middleware("http")
async def track_storage_operations(request: Request, call_next):
    """Middleware to track storage operation latency and performance"""
    start_time = time.time()
    
    # Determine operation type based on endpoint
    operation_type = "UNKNOWN"
    bucket = None
    
    path = request.url.path
    if path == "/" or path == "/config" or path.endswith("/config.html"):
        operation_type = "CONFIG_PAGE"
    elif path == "/dashboard" or path.endswith("/index.html"):
        operation_type = "PAGE_LOAD"
    elif "/static/" in path:
        operation_type = "STATIC_ASSET"
    elif "/health" in path:
        operation_type = "HEALTH_CHECK"
    elif "/stats" in path:
        operation_type = "STATS"
    elif "/tools/buckets" in path or "/metrics/sources" in path:
        operation_type = "LIST_BUCKETS"
    elif "/tools/list-all" in path or "/tools/storage-summary" in path:
        operation_type = "LIST_OBJECTS"
        bucket = request.query_params.get("bucket")
    elif "/tools/top-largest" in path:
        operation_type = "ANALYZE_STORAGE"
        bucket = request.query_params.get("bucket")
    elif "/chat" in path:
        operation_type = "AI_QUERY"
    elif "recent_objects" in path or path.startswith("/metrics/"):
        operation_type = "QUERY_METRICS"
    elif "/logs" in path or "access_logs" in path or "search_logs" in path:
        operation_type = "QUERY_LOGS"
    elif "/plots/" in path:
        operation_type = "GENERATE_PLOT"
    
    try:
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000
        
        # Estimate bytes transferred from Content-Length header
        bytes_transferred = 0
        if "content-length" in response.headers:
            try:
                bytes_transferred = int(response.headers["content-length"])
            except:
                pass
        
        # Record storage operation metrics
        record_storage_operation(
            endpoint=path,
            method=request.method,
            operation_type=operation_type,
            duration_ms=duration_ms,
            status_code=response.status_code,
            bucket=bucket,
            bytes_transferred=bytes_transferred
        )
        
        # Add custom headers with metrics (like S3 request metrics)
        response.headers["X-Request-Duration-Ms"] = str(round(duration_ms, 2))
        response.headers["X-Request-Id"] = request.headers.get("X-Request-Id", f"req-{int(time.time() * 1000)}")
        response.headers["X-Operation-Type"] = operation_type
        
        # Structured logging for storage operations
        logger.info(
            f"STORAGE_OP: {operation_type} | {request.method} {path} | "
            f"status={response.status_code} | duration={duration_ms:.2f}ms | "
            f"bytes={bytes_transferred} | bucket={bucket or 'N/A'}"
        )
        
        return response
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            f"STORAGE_OP_ERROR: {operation_type} | {request.method} {path} | "
            f"duration={duration_ms:.2f}ms | error={str(e)}"
        )
        raise

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def home():
    """Serve the configuration page as the landing page"""
    if os.path.exists("static/config.html"):
        return FileResponse("static/config.html")
    return {"ok": True, "message": "SpaceWatch backend running"}


@app.get("/dashboard")
def dashboard():
    """Serve the main dashboard after configuration"""
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"ok": True, "message": "SpaceWatch dashboard not found"}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"detail": f"Internal Server Error: {type(exc).__name__}: {str(exc)}"})


# ============================================================
# MODELS
# ============================================================
class ChatRequest(BaseModel):
    message: str
    spaces_key: SecretStr
    spaces_secret: SecretStr
    log_bucket: Optional[str] = None  # User's bucket for access logs
    log_prefix: Optional[str] = ""    # Optional prefix for access logs
    metrics_bucket: Optional[str] = None  # User's bucket for metrics
    metrics_prefix: Optional[str] = "spacewatch-metrics/"
    region: Optional[str] = None
    endpoint: Optional[str] = None


# ============================================================
# RATE LIMIT + MEMORY (in-memory)
# ============================================================
@dataclass
class TokenBucket:
    tokens: float
    last_ts: float

@dataclass
class MemoryState:
    last_bucket: Optional[str]
    last_tool: Optional[str]
    last_updated_ts: float

BUCKET_BY_IP: Dict[str, TokenBucket] = {}
MEMORY_BY_KEY: Dict[str, MemoryState] = {}

STATS = {
    "chat_requests": 0,
    "tool_requests": 0,
    "agent_calls": 0,
    "agent_failures": 0,
    "bucket_cache_refreshes": 0,
    "scheduler_runs": 0,
    "scheduler_snapshots_ok": 0,
    "scheduler_snapshots_err": 0,
}

# ============================================================
# ENHANCED STORAGE METRICS (S3/Azure Blob style)
# ============================================================
@dataclass
class StorageOperationMetrics:
    """Track storage operation latency and performance metrics"""
    start_time: float
    endpoint: str
    method: str
    operation_type: str  # GET, PUT, DELETE, HEAD, LIST
    bucket: Optional[str] = None
    status_code: int = 0
    duration_ms: float = 0.0
    bytes_transferred: int = 0
    
# Metrics storage for storage operation tracking
STORAGE_OPERATION_METRICS: List[StorageOperationMetrics] = []
MAX_STORAGE_METRICS_SAMPLES = 10000  # Keep last 10k storage operations
MAX_LATENCY_SAMPLES = 10000  # Keep last 10k requests for latency tracking

def record_storage_operation(
    endpoint: str, 
    method: str, 
    operation_type: str,
    duration_ms: float, 
    status_code: int,
    bucket: Optional[str] = None,
    bytes_transferred: int = 0
):
    """Record storage operation metrics for analytics"""
    metric = StorageOperationMetrics(
        start_time=time.time(),
        endpoint=endpoint,
        method=method,
        operation_type=operation_type,
        bucket=bucket,
        status_code=status_code,
        duration_ms=duration_ms,
        bytes_transferred=bytes_transferred
    )
    STORAGE_OPERATION_METRICS.append(metric)
    
    # Keep only recent samples
    if len(STORAGE_OPERATION_METRICS) > MAX_STORAGE_METRICS_SAMPLES:
        STORAGE_OPERATION_METRICS.pop(0)
    
    # Log slow operations (like S3 slow request logs)
    if duration_ms > 5000:  # > 5 seconds
        logger.warning(
            f"SLOW_OPERATION: {operation_type} {endpoint} - {duration_ms:.2f}ms - "
            f"bucket={bucket} status={status_code}"
        )

def calculate_percentiles(values: List[float], percentiles: List[int] = [50, 95, 99]) -> Dict[str, float]:
    """Calculate percentiles for latency metrics"""
    if not values:
        return {f"p{p}": 0.0 for p in percentiles}
    
    sorted_vals = sorted(values)
    result = {}
    for p in percentiles:
        idx = int(len(sorted_vals) * p / 100)
        if idx >= len(sorted_vals):
            idx = len(sorted_vals) - 1
        result[f"p{p}"] = sorted_vals[idx]
    return result

def get_storage_metrics() -> Dict[str, Any]:
    """Get storage operation metrics (S3/Azure Blob style analytics)"""
    now = time.time()
    recent_window = 300  # 5 minutes
    recent_ops = [op for op in STORAGE_OPERATION_METRICS if now - op.start_time <= recent_window]
    
    total_ops = len(recent_ops)
    error_ops = len([op for op in recent_ops if op.status_code >= 400])
    
    # Break down by operation type
    ops_by_type = defaultdict(lambda: {"count": 0, "errors": 0, "latencies": [], "bytes": 0})
    for op in recent_ops:
        stats = ops_by_type[op.operation_type]
        stats["count"] += 1
        if op.status_code >= 400:
            stats["errors"] += 1
        stats["latencies"].append(op.duration_ms)
        stats["bytes"] += op.bytes_transferred
    
    # Calculate overall metrics
    all_latencies = [op.duration_ms for op in recent_ops]
    percentiles = calculate_percentiles(all_latencies)
    
    # Operation breakdown
    operation_breakdown = {}
    for op_type, stats in ops_by_type.items():
        operation_breakdown[op_type] = {
            "request_count": stats["count"],
            "error_count": stats["errors"],
            "error_rate_percent": round(stats["errors"] / stats["count"] * 100, 2) if stats["count"] > 0 else 0,
            "bytes_transferred": stats["bytes"],
            **calculate_percentiles(stats["latencies"])
        }
    
    # Calculate operation rate
    ops_per_second = total_ops / recent_window if recent_window > 0 else 0
    error_rate = (error_ops / total_ops * 100) if total_ops > 0 else 0
    
    # Calculate data transfer metrics
    total_bytes_transferred = sum(op.bytes_transferred for op in recent_ops)
    bytes_per_second = total_bytes_transferred / recent_window if recent_window > 0 else 0
    
    return {
        "total_operations_5m": total_ops,
        "error_operations_5m": error_ops,
        "error_rate_percent": round(error_rate, 2),
        "operations_per_second": round(ops_per_second, 2),
        "total_bytes_transferred_5m": total_bytes_transferred,
        "bytes_per_second": round(bytes_per_second, 2),
        "latency_p50_ms": round(percentiles.get("p50", 0), 2),
        "latency_p95_ms": round(percentiles.get("p95", 0), 2),
        "latency_p99_ms": round(percentiles.get("p99", 0), 2),
        "avg_latency_ms": round(sum(all_latencies) / len(all_latencies), 2) if all_latencies else 0,
        "operation_breakdown": operation_breakdown,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ============================================================
# TOP IPs IMAGE CACHE (per-credential cached for BYOC safety)
# ============================================================
# Structure: { (credential_cache_key, bucket, date, limit): (timestamp, png_bytes) }
TOP_IPS_CACHE: Dict[Tuple[str, str, str, int], Tuple[float, bytes]] = {}
TOP_IPS_TTL = 120    # seconds

def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"

def memory_key(request: Request, x_session_id: Optional[str], credential_cache_key: str) -> str:
    """
    Generate a memory key for session/IP tracking that includes credential isolation.
    
    In BYOC scenarios, we must ensure different credentials don't share memory state,
    even if they're from the same IP or session. This prevents cross-user data leakage.
    """
    base_key = ""
    if x_session_id and x_session_id.strip():
        base_key = f"sess:{x_session_id.strip()}"
    else:
        base_key = f"ip:{client_ip(request)}"
    
    # Include credential cache key to isolate memory by credentials
    return f"{base_key}:cred:{credential_cache_key}"

def rate_limit(ip: str):
    now = time.time()
    b = BUCKET_BY_IP.get(ip)
    if not b:
        b = TokenBucket(tokens=float(RATE_LIMIT_BURST), last_ts=now)
        BUCKET_BY_IP[ip] = b

    elapsed = now - b.last_ts
    b.tokens = min(float(RATE_LIMIT_BURST), b.tokens + elapsed * RATE_LIMIT_RPS)
    b.last_ts = now

    if b.tokens < 1.0:
        raise HTTPException(status_code=429, detail="Too Many Requests")
    b.tokens -= 1.0

def update_memory(key: str, bucket: Optional[str], tool: Optional[str]):
    MEMORY_BY_KEY[key] = MemoryState(last_bucket=bucket, last_tool=tool, last_updated_ts=time.time())

def get_memory(key: str) -> Optional[MemoryState]:
    st = MEMORY_BY_KEY.get(key)
    if not st:
        return None
    if time.time() - st.last_updated_ts > 1800:
        MEMORY_BY_KEY.pop(key, None)
        return None
    return st


# ============================================================
# AUTH
# ============================================================
def require_api_key(x_api_key: Optional[str]):
    if APP_API_KEY and x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-API-Key)")


# ============================================================
# BUCKET DISCOVERY (per-credential cached for BYOC safety)
# ============================================================
# Structure: { credential_cache_key: (timestamp, bucket_set, last_error) }
_BUCKET_CACHE_BY_CREDENTIAL: Dict[str, Tuple[float, Set[str], Optional[str]]] = {}

def _seed_known_buckets(log_bucket: Optional[str] = None, metrics_bucket: Optional[str] = None) -> Set[str]:
    """
    If list_buckets is not permitted (common with scoped keys),
    allow explicitly configured buckets to still pass require_bucket_allowed().
    """
    buckets = set()
    if log_bucket:
        buckets.add(log_bucket)
    if metrics_bucket:
        buckets.add(metrics_bucket)
    if SCHEDULER_SOURCE_BUCKETS:
        buckets.update(SCHEDULER_SOURCE_BUCKETS)
    if FALLBACK_BUCKETS:
        buckets.update(FALLBACK_BUCKETS)
    return buckets


def refresh_bucket_cache(
    s3_client, 
    credential_cache_key: str,
    force: bool = False, 
    log_bucket: Optional[str] = None, 
    metrics_bucket: Optional[str] = None
) -> Set[str]:
    """
    Refresh bucket cache for a specific credential set (BYOC-safe).
    
    Each credential gets its own cache entry to prevent cross-user bucket leakage.
    The credential_cache_key should be generated using get_credential_cache_key().
    """
    now = time.time()
    
    # Check if we have a valid cached entry for this credential
    cached_entry = _BUCKET_CACHE_BY_CREDENTIAL.get(credential_cache_key)
    if not force and cached_entry:
        cache_ts, cached_buckets, _ = cached_entry
        if cached_buckets and (now - cache_ts) < BUCKET_CACHE_TTL_SEC:
            return cached_buckets

    STATS["bucket_cache_refreshes"] += 1
    last_error = None
    
    try:
        resp = s3_client.list_buckets()
        buckets = {b["Name"] for b in resp.get("Buckets", []) if b.get("Name")}
        if not buckets:
            buckets = _seed_known_buckets(log_bucket, metrics_bucket)
        _BUCKET_CACHE_BY_CREDENTIAL[credential_cache_key] = (now, buckets, None)
        return buckets
    except ClientError as e:
        last_error = f"{e}"
    except Exception as e:
        last_error = str(e)

    # If list_buckets is disallowed, fall back to explicitly configured buckets
    # so require_bucket_allowed() will still work for scoped keys.
    buckets = _seed_known_buckets(log_bucket, metrics_bucket)
    _BUCKET_CACHE_BY_CREDENTIAL[credential_cache_key] = (now, buckets, last_error)
    return buckets


def require_bucket_allowed(
    bucket: str, 
    s3_client, 
    credential_cache_key: str,
    log_bucket: Optional[str] = None, 
    metrics_bucket: Optional[str] = None
):
    """
    Verify that a bucket is accessible with the provided credentials (BYOC-safe).
    
    Uses per-credential caching to prevent cross-user bucket leakage.
    """
    buckets = refresh_bucket_cache(
        s3_client, 
        credential_cache_key=credential_cache_key,
        log_bucket=log_bucket, 
        metrics_bucket=metrics_bucket
    )
    if not buckets:
        raise HTTPException(
            status_code=403,
            detail="Cannot list buckets with these Spaces credentials. Use unscoped key or set FALLBACK_BUCKETS.",
        )
    if bucket not in buckets:
        raise HTTPException(status_code=403, detail=f"Bucket not allowed/not found: {bucket}")

def normalize_prefix(p: str) -> str:
    p = (p or "").strip()
    if p and not p.endswith("/"):
        p += "/"
    return p

def bytes_to_human(n: int) -> str:
    size = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} EB"

def list_objects(
    s3_client, 
    bucket: str, 
    credential_cache_key: str,
    prefix: str = "", 
    max_items: Optional[int] = None, 
    log_bucket: Optional[str] = None, 
    metrics_bucket: Optional[str] = None
) -> List[Dict[str, Any]]:
    """List objects in a bucket with per-credential cache validation (BYOC-safe)."""
    require_bucket_allowed(bucket, s3_client, credential_cache_key, log_bucket, metrics_bucket)
    paginator = s3_client.get_paginator("list_objects_v2")
    out: List[Dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out.append({
                "key": o["Key"],
                "size_bytes": int(o["Size"]),
                "size_human": bytes_to_human(int(o["Size"])),
                "last_modified": o["LastModified"].astimezone(timezone.utc).isoformat(),
            })
            if max_items and len(out) >= max_items:
                return out
    return out

def recent_objects(
    s3_client, 
    bucket: str, 
    credential_cache_key: str,
    prefix: str = "", 
    limit: int = 10, 
    log_bucket: Optional[str] = None, 
    metrics_bucket: Optional[str] = None
) -> Dict[str, Any]:
    """
    Return most recently modified objects.
    NOTE: S3 listing isn't guaranteed to be ordered by last_modified, so we sort.
    """
    limit = max(1, min(int(limit), 200))
    objs = list_objects(s3_client, bucket, credential_cache_key, prefix=prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    # sort by last_modified ISO string (UTC ISO sorts lexicographically)
    objs.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
    top = objs[:limit]
    return {
        "bucket": bucket,
        "prefix": prefix,
        "count_scanned": len(objs),
        "limit": limit,
        "objects": top,
        "note": "Sorted by last_modified to find the most recent objects.",
    }

def safe_tail_lines(text: str, max_lines: int) -> List[str]:
    lines = text.splitlines()
    return lines if len(lines) <= max_lines else lines[-max_lines:]


# ============================================================
# ACCESS LOG PARSING
# ============================================================
_S3_TIME_RE = re.compile(r"\[(?P<ts>\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+\-]\d{4})\]")
_REQ_RE = re.compile(r'"(?P<method>[A-Z]+)\s+(?P<path>[^ ]+)\s+(?P<proto>[^"]+)"')
_AFTER_REQ_RE = re.compile(r'"\s+(?P<status>\d{3})\s+(?P<err>\S+)\s+(?P<bytes>\d+|-)')
_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")

def looks_like_log_key(key: str) -> bool:
    k = key.lower()
    return ".log" in k and (k.endswith(".gz") or k.endswith(".log"))

def extract_ip_from_access_log_line(line: str) -> Optional[str]:
    parts = line.split()
    if len(parts) >= 2:
        candidate = parts[1]
        if _IP_RE.fullmatch(candidate):
            return candidate
    m = _IP_RE.search(line)
    return m.group(0) if m else None

def parse_do_s3_access_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        ip = extract_ip_from_access_log_line(line)
        ts = None
        m_ts = _S3_TIME_RE.search(line)
        if m_ts:
            ts = datetime.strptime(m_ts.group("ts"), "%d/%b/%Y:%H:%M:%S %z").astimezone(timezone.utc)

        m_req = _REQ_RE.search(line)
        if not m_req:
            return None

        method = m_req.group("method")
        status = None
        bytes_sent = 0

        m_after = _AFTER_REQ_RE.search(line[m_req.end():])
        if m_after:
            status = int(m_after.group("status"))
            b = m_after.group("bytes")
            bytes_sent = int(b) if b.isdigit() else 0
        else:
            tail = line[m_req.end():].strip().split()
            if len(tail) >= 1 and tail[0].isdigit():
                status = int(tail[0])
            for tok in tail[1:5]:
                if tok.isdigit():
                    bytes_sent = int(tok)
                    break

        return {"ts": ts, "method": method, "status": status, "bytes_sent": bytes_sent, "ip": ip, "raw": line}
    except Exception:
        return None


# ============================================================
# METRICS SNAPSHOT STORAGE (JSONL in Spaces)
# ============================================================
def _metrics_key_for_ts(ts: datetime, metrics_prefix: str = "") -> str:
    dt = ts.strftime("%Y-%m-%d")
    hh = ts.strftime("%H")
    prefix = normalize_prefix(metrics_prefix)
    ext = "jsonl.gz" if METRICS_GZIP else "jsonl"
    return f"{prefix}dt={dt}/hour={hh}/metrics.{ext}"

def _put_metrics_line(s3_client, bucket: str, key: str, line: str, log_bucket: Optional[str] = None, metrics_bucket: Optional[str] = None):
    require_bucket_allowed(bucket, s3_client, log_bucket, metrics_bucket)

    existing = b""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        existing = obj["Body"].read()
        if METRICS_GZIP and key.endswith(".gz"):
            existing = gzip.decompress(existing)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in {"NoSuchKey", "404"}:
            raise

    new_payload = existing + (line.rstrip("\n") + "\n").encode("utf-8")
    body = gzip.compress(new_payload) if (METRICS_GZIP and key.endswith(".gz")) else new_payload
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

def storage_summary(
    s3_client, 
    bucket: str, 
    credential_cache_key: str,
    prefix: str = "", 
    log_bucket: Optional[str] = None, 
    metrics_bucket: Optional[str] = None
) -> Dict[str, Any]:
    """Get storage summary with per-credential cache validation (BYOC-safe)."""
    objs = list_objects(s3_client, bucket, credential_cache_key, prefix=prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    total = sum(o["size_bytes"] for o in objs)
    return {
        "bucket": bucket,
        "prefix": prefix,
        "object_count": len(objs),
        "total_size_bytes": total,
        "total_size_human": bytes_to_human(total),
    }

def compute_request_metrics_from_logs(
    s3_client, 
    source_bucket: str, 
    credential_cache_key: str,
    log_bucket: Optional[str], 
    log_prefix: str = ""
) -> Dict[str, Any]:
    """Compute request metrics from access logs (BYOC-safe)."""
    if not log_bucket:
        return {"requests_total": 0, "note": "log_bucket not set"}

    root = normalize_prefix(log_prefix)
    objs = list_objects(s3_client, log_bucket, credential_cache_key, prefix=root, log_bucket=log_bucket, metrics_bucket=None)

    src = source_bucket.lower().strip()
    candidates = [o for o in objs if src in o["key"].lower() and looks_like_log_key(o["key"])]
    candidates.sort(key=lambda x: x["last_modified"], reverse=True)
    candidates = candidates[:MAX_LOG_FILES_SCAN]

    totals = {
        "requests_total": 0,
        "requests_get": 0,
        "requests_put": 0,
        "requests_delete": 0,
        "requests_head": 0,
        "status_2xx": 0,
        "status_3xx": 0,
        "status_4xx": 0,
        "status_5xx": 0,
        "bytes_sent": 0,
        "unique_ips": 0,
        "top_ips": [],
        "files_scanned": 0,
        "lines_scanned": 0,
    }

    if not candidates:
        return totals

    ip_counts = Counter()
    lines_scanned = 0
    files_scanned = 0

    for o in candidates:
        key = o["key"]
        try:
            obj = s3_client.get_object(Bucket=log_bucket, Key=key)
            raw = obj["Body"].read(MAX_LOG_BYTES + 1)
            if len(raw) > MAX_LOG_BYTES:
                continue
            if key.lower().endswith(".gz"):
                raw = gzip.decompress(raw)

            text = raw.decode("utf-8", errors="replace")
            for line in text.splitlines():
                if not line:
                    continue
                parsed = parse_do_s3_access_line(line)
                if not parsed:
                    continue

                totals["requests_total"] += 1
                m = parsed["method"]
                if m == "GET": totals["requests_get"] += 1
                elif m == "PUT": totals["requests_put"] += 1
                elif m == "DELETE": totals["requests_delete"] += 1
                elif m == "HEAD": totals["requests_head"] += 1

                st = parsed["status"]
                if isinstance(st, int):
                    if 200 <= st < 300: totals["status_2xx"] += 1
                    elif 300 <= st < 400: totals["status_3xx"] += 1
                    elif 400 <= st < 500: totals["status_4xx"] += 1
                    elif 500 <= st < 600: totals["status_5xx"] += 1

                totals["bytes_sent"] += int(parsed["bytes_sent"] or 0)

                ip = parsed.get("ip")
                if ip:
                    ip_counts[ip] += 1

                lines_scanned += 1
                if lines_scanned >= MAX_LOG_LINES_SCAN:
                    break

            files_scanned += 1
            if lines_scanned >= MAX_LOG_LINES_SCAN:
                break
        except Exception:
            continue

    totals["files_scanned"] = files_scanned
    totals["lines_scanned"] = lines_scanned
    totals["unique_ips"] = len(ip_counts)
    totals["top_ips"] = [{"ip": ip, "count": c} for ip, c in ip_counts.most_common(10)]
    return totals

def run_metrics_snapshot(s3_client, source_bucket: str, credential_cache_key: str, source_prefix: str = "", log_bucket: Optional[str] = None, log_prefix: str = "", metrics_bucket: Optional[str] = None, metrics_prefix: str = "") -> Dict[str, Any]:
    if not log_bucket:
        raise HTTPException(status_code=400, detail="log_bucket not set")
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="metrics_bucket not set")

    ts = datetime.now(timezone.utc)
    logs_prefix = normalize_prefix(log_prefix)

    src_summary = storage_summary(s3_client, bucket=source_bucket, credential_cache_key=credential_cache_key, prefix=source_prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)

    objs = list_objects(s3_client, log_bucket, credential_cache_key, prefix=logs_prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    src = source_bucket.lower().strip()
    logs_files = 0
    logs_bytes = 0
    for o in objs:
        if src in o["key"].lower() and looks_like_log_key(o["key"]):
            logs_files += 1
            logs_bytes += int(o["size_bytes"])

    record = {
        "ts": ts.isoformat(),
        "source_bucket": source_bucket,
        "source_prefix": source_prefix or "",
        "source_objects": int(src_summary["object_count"]),
        "source_bytes": int(src_summary["total_size_bytes"]),
        "logs_bucket": log_bucket,
        "logs_prefix": logs_prefix,
        "logs_files": int(logs_files),
        "logs_bytes": int(logs_bytes),
    }
    record.update(compute_request_metrics_from_logs(s3_client, source_bucket, credential_cache_key, log_bucket, log_prefix))

    key = _metrics_key_for_ts(ts, metrics_prefix)
    _put_metrics_line(s3_client, metrics_bucket, key, json.dumps(record, separators=(",", ":")), log_bucket, metrics_bucket)
    return {"metrics_bucket": metrics_bucket, "metrics_key": key, "record": record}


# ============================================================
# METRICS READERS
# ============================================================
METRICS_MAX_POINTS = int(os.getenv("METRICS_MAX_POINTS", "2000"))

def metrics_sources_internal(s3_client, metrics_bucket: Optional[str], credential_cache_key: str, metrics_prefix: str = "", hours: int = 168, log_bucket: Optional[str] = None) -> Dict[str, Any]:
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="metrics_bucket not set")
    hours = max(1, min(int(hours), 720))
    metrics_pfx = normalize_prefix(metrics_prefix)
    objs = list_objects(s3_client, metrics_bucket, credential_cache_key, prefix=metrics_pfx, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    objs.sort(key=lambda x: x["last_modified"], reverse=True)

    cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
    keep = []
    for o in objs:
        try:
            lm = datetime.fromisoformat(o["last_modified"].replace("Z", "+00:00"))
            if lm.timestamp() >= cutoff:
                keep.append(o)
        except Exception:
            keep.append(o)
    keep = keep[:200]

    sources = set()
    for o in keep:
        key = o["key"]
        try:
            obj = s3_client.get_object(Bucket=metrics_bucket, Key=key)
            raw = obj["Body"].read(MAX_LOG_BYTES + 1)
            if len(raw) > MAX_LOG_BYTES:
                continue
            if key.endswith(".gz"):
                raw = gzip.decompress(raw)
            text = raw.decode("utf-8", errors="replace")
            for line in text.splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                b = (rec.get("source_bucket") or "").strip()
                if b:
                    sources.add(b)
        except Exception:
            continue

    out = sorted(sources)
    return {"count": len(out), "sources": out, "metrics_bucket": metrics_bucket, "metrics_prefix": metrics_pfx}

def metrics_series_internal(s3_client, source_bucket: str, metrics_bucket: Optional[str], credential_cache_key: str, metrics_prefix: str = "", source_prefix: str = "", limit: int = 500, hours: int = 168, log_bucket: Optional[str] = None) -> Dict[str, Any]:
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="metrics_bucket not set")
    limit = max(1, min(int(limit), METRICS_MAX_POINTS))
    hours = max(1, min(int(hours), 720))

    want_bucket = (source_bucket or "").strip()
    want_bucket_l = want_bucket.lower()
    want_prefix = (source_prefix or "").strip()

    metrics_pfx = normalize_prefix(metrics_prefix)
    objs = list_objects(s3_client, metrics_bucket, credential_cache_key, prefix=metrics_pfx, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    objs.sort(key=lambda x: x["last_modified"], reverse=True)

    cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
    keep = []
    for o in objs:
        try:
            lm = datetime.fromisoformat(o["last_modified"].replace("Z", "+00:00"))
            if lm.timestamp() >= cutoff:
                keep.append(o)
        except Exception:
            keep.append(o)
    keep = keep[:200]

    points = []
    for o in reversed(keep):
        key = o["key"]
        try:
            obj = s3_client.get_object(Bucket=metrics_bucket, Key=key)
            raw = obj["Body"].read(MAX_LOG_BYTES + 1)
            if len(raw) > MAX_LOG_BYTES:
                continue
            if key.endswith(".gz"):
                raw = gzip.decompress(raw)
            text = raw.decode("utf-8", errors="replace")

            for line in text.splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if (rec.get("source_bucket") or "").lower() != want_bucket_l:
                    continue
                if (rec.get("source_prefix") or "").strip() != want_prefix:
                    continue
                if not rec.get("ts"):
                    continue
                points.append(rec)
        except Exception:
            continue

    points.sort(key=lambda p: p.get("ts") or "")
    if len(points) > limit:
        points = points[-limit:]
    return {"source_bucket": want_bucket, "source_prefix": want_prefix, "count": len(points), "points": points}

# ============================================================
# METRICS HTTP ENDPOINTS (needed by frontend dashboard)
# ============================================================
@app.get("/metrics/sources")
def http_metrics_sources(
    hours: int = 168,
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    """
    Frontend bucket discovery endpoint.
    Returns list of source buckets discovered from stored snapshot objects.
    """
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="X-Metrics-Bucket header required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    return metrics_sources_internal(s3_client, metrics_bucket, credential_cache_key, metrics_prefix or "", hours=hours, log_bucket=log_bucket)


@app.get("/metrics/series")
def http_metrics_series(
    source_bucket: str,
    source_prefix: str = "",
    limit: int = 500,
    hours: int = 168,
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    """
    Frontend timeseries endpoint for a specific bucket/prefix.
    """
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="X-Metrics-Bucket header required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    return metrics_series_internal(
        s3_client=s3_client,
        source_bucket=source_bucket,
        metrics_bucket=metrics_bucket,
        credential_cache_key=credential_cache_key,
        metrics_prefix=metrics_prefix or "",
        source_prefix=source_prefix,
        limit=limit,
        hours=hours,
        log_bucket=log_bucket,
    )


@app.get("/metrics/aggregate-series")
def http_metrics_aggregate_series(
    hours: int = 24,
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    """
    Optional (but many dashboards use this):
    Aggregates totals across all buckets at each timestamp from stored snapshot records.
    Returns points with summed source_bytes/logs_bytes/source_objects/logs_files and request stats if present.
    """
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="X-Metrics-Bucket header required")

    hours = max(1, min(int(hours), 168))  # cap at 7 days
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)

    # Discover buckets from snapshots, then pull series per bucket and aggregate by ts.
    srcs = metrics_sources_internal(s3_client, metrics_bucket, credential_cache_key, metrics_prefix or "", hours=hours, log_bucket=log_bucket).get("sources") or []
    agg: Dict[str, Dict[str, int]] = defaultdict(lambda: {
        "source_bytes": 0,
        "logs_bytes": 0,
        "source_objects": 0,
        "logs_files": 0,
        "requests_total": 0,
        "bytes_sent": 0,
        "status_4xx": 0,
        "status_5xx": 0,
    })

    for b in srcs:
        series = metrics_series_internal(s3_client, source_bucket=b, metrics_bucket=metrics_bucket, credential_cache_key=credential_cache_key, metrics_prefix=metrics_prefix or "", source_prefix="", hours=hours, limit=2000, log_bucket=log_bucket)
        for rec in series.get("points", []):
            ts = rec.get("ts")
            if not ts:
                continue
            a = agg[ts]
            a["source_bytes"] = int(rec.get("source_bytes", 0) or 0)
            a["logs_bytes"] = int(rec.get("logs_bytes", 0) or 0)
            a["source_objects"] = int(rec.get("source_objects", 0) or 0)
            a["logs_files"] = int(rec.get("logs_files", 0) or 0)
            a["requests_total"] = int(rec.get("requests_total", 0) or 0)
            a["bytes_sent"] = int(rec.get("bytes_sent", 0) or 0)
            a["status_4xx"] = int(rec.get("status_4xx", 0) or 0)
            a["status_5xx"] = int(rec.get("status_5xx", 0) or 0)

    points = [{"ts": ts, **vals} for ts, vals in sorted(agg.items(), key=lambda x: x[0])]
    return {
        "hours": hours,
        "count": len(points),
        "points": points,
        "sources_count": len(srcs),
        "note": "Aggregated totals across discovered source buckets from stored snapshot points.",
    }

# ============================================================
# COMPATIBILITY TOOL ROUTES (for existing frontend)
# Your frontend is calling /tools/buckets -> add it back.
# ============================================================
@app.get("/tools/buckets")
def tool_buckets(
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    STATS["tool_requests"] += 1

    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    buckets = sorted(list(refresh_bucket_cache(s3_client, credential_cache_key, force=True, log_bucket=log_bucket, metrics_bucket=metrics_bucket)))
    if not buckets:
        raise HTTPException(
            status_code=403,
            detail=(
                "Cannot list buckets with current Spaces credentials. "
                "Use an unscoped key that can list buckets, or set FALLBACK_BUCKETS."
            ),
        )
    return {"bucket_count": len(buckets), "buckets": buckets}


@app.get("/tools/storage-summary")
def tool_storage_summary(
    bucket: str,
    prefix: str = "",
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    STATS["tool_requests"] += 1
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    return storage_summary(s3_client, bucket, prefix, log_bucket, metrics_bucket)


@app.get("/tools/list-all")
def tool_list_all(
    bucket: str,
    prefix: str = "",
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    STATS["tool_requests"] += 1
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    objs = list_objects(s3_client, bucket, prefix=prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    return {
        "bucket": bucket,
        "prefix": prefix,
        "count": len(objs),
        "objects": objs[:MAX_LIST_OBJECTS_RETURN],
        "truncated": len(objs) > MAX_LIST_OBJECTS_RETURN,
    }


@app.get("/tools/top-largest")
def tool_top_largest(
    bucket: str,
    limit: int = 10,
    prefix: str = "",
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    STATS["tool_requests"] += 1
    limit = max(1, min(int(limit), MAX_TOP_LARGEST))
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    objs = list_objects(s3_client, bucket, prefix=prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    objs.sort(key=lambda x: x["size_bytes"], reverse=True)
    return {"bucket": bucket, "prefix": prefix, "limit": limit, "objects": objs[:limit]}



# ============================================================
# LOGS: list/read/search + object audit timeline
# ============================================================
def read_log_object(s3_client, bucket: str, key: str, credential_cache_key: str, tail_lines: int = 200, log_bucket: Optional[str] = None, metrics_bucket: Optional[str] = None) -> Dict[str, Any]:
    require_bucket_allowed(bucket, s3_client, credential_cache_key, log_bucket, metrics_bucket)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read(MAX_LOG_BYTES + 1)
    if len(raw) > MAX_LOG_BYTES:
        raise HTTPException(status_code=413, detail="Log file too large")
    if key.lower().endswith(".gz"):
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", errors="replace")
    lines = safe_tail_lines(text, min(int(tail_lines), MAX_LOG_LINES))
    return {"bucket": bucket, "key": key, "returned_lines": len(lines), "log_lines": lines}

def list_access_log_objects_for_source(s3_client, source_bucket: str, log_bucket: Optional[str], credential_cache_key: str, log_prefix: str = "", date_yyyy_mm_dd: Optional[str] = None, max_items: int = 200, metrics_bucket: Optional[str] = None) -> List[Dict[str, Any]]:
    if not log_bucket:
        raise HTTPException(status_code=400, detail="log_bucket not set")
    root = normalize_prefix(log_prefix)
    objs = list_objects(s3_client, log_bucket, credential_cache_key, prefix=root, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    src = source_bucket.lower().strip()
    out = []
    for o in objs:
        k = o["key"]
        if src in k.lower() and looks_like_log_key(k):
            if date_yyyy_mm_dd and date_yyyy_mm_dd not in k:
                continue
            out.append(o)
    out.sort(key=lambda x: x["last_modified"], reverse=True)
    return out[:max(1, min(int(max_items), 500))]

def search_access_logs(
    s3_client,
    source_bucket: str,
    log_bucket: Optional[str],
    credential_cache_key: str,
    log_prefix: str = "",
    contains: Optional[str] = None,
    method: Optional[str] = None,
    object_key: Optional[str] = None,
    status_prefix: Optional[str] = None,  # "2","4","5"
    date_yyyy_mm_dd: Optional[str] = None,
    limit_matches: int = 20,
    metrics_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Searches .log/.log.gz for matches. Returns parsed matches with ip/method/status/ts and the log object ref.
    """
    candidates = list_access_log_objects_for_source(s3_client, source_bucket, log_bucket, credential_cache_key, log_prefix, date_yyyy_mm_dd=date_yyyy_mm_dd, max_items=MAX_LOG_FILES_SCAN, metrics_bucket=metrics_bucket)
    want_method = (method or "").upper().strip() or None
    want_contains = contains
    want_key = object_key
    want_key_alt = None
    if want_key and " " in want_key:
        want_key_alt = want_key.replace(" ", "%20")

    out = []
    files_scanned = 0
    lines_scanned = 0

    for o in candidates:
        log_key = o["key"]
        try:
            obj = s3_client.get_object(Bucket=log_bucket, Key=log_key)
            raw = obj["Body"].read(MAX_LOG_BYTES + 1)
            if len(raw) > MAX_LOG_BYTES:
                continue
            if log_key.lower().endswith(".gz"):
                raw = gzip.decompress(raw)
            text = raw.decode("utf-8", errors="replace")

            for line in text.splitlines():
                if not line:
                    continue
                if want_contains and want_contains not in line:
                    continue
                if want_key and (want_key not in line) and (want_key_alt and want_key_alt not in line):
                    continue
                if want_method and (f'"{want_method} ' not in line):
                    continue

                parsed = parse_do_s3_access_line(line)
                if not parsed:
                    continue

                if status_prefix:
                    st = parsed.get("status")
                    if not (isinstance(st, int) and str(st).startswith(str(status_prefix))):
                        continue

                out.append({
                    "ip": parsed.get("ip"),
                    "ts": parsed["ts"].isoformat() if parsed.get("ts") else None,
                    "method": parsed.get("method"),
                    "status": parsed.get("status"),
                    "bytes_sent": parsed.get("bytes_sent"),
                    "log_object": {"bucket": log_bucket, "key": log_key},
                    "raw": parsed.get("raw", "")[:3000],
                })

                if len(out) >= int(limit_matches):
                    break

                lines_scanned += 1
                if lines_scanned >= MAX_LOG_LINES_SCAN:
                    break

            files_scanned += 1
            if len(out) >= int(limit_matches) or lines_scanned >= MAX_LOG_LINES_SCAN:
                break
        except Exception:
            continue

    out.sort(key=lambda x: x["ts"] or "", reverse=True)
    return {
        "source_bucket": source_bucket,
        "date": date_yyyy_mm_dd,
        "count": len(out),
        "matches": out,
        "files_scanned": files_scanned,
        "lines_scanned": lines_scanned,
        "note": "Matches are best-effort based on scanned recent log files.",
    }

def object_audit_timeline(s3_client, source_bucket: str, object_key: str, log_bucket: Optional[str], credential_cache_key: str, log_prefix: str = "", hours: int = 168, limit: int = 50, methods: Optional[List[str]] = None, metrics_bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    CloudTrail-ish timeline for a specific object:
    - searches access logs for that object key across recent files
    - optionally filters by methods (PUT/GET/DELETE/HEAD)
    """
    methods = methods or ["PUT", "GET", "DELETE", "HEAD"]
    methods = [m.upper().strip() for m in methods if m and isinstance(m, str)]
    methods = methods[:10]

    # You can tighten by date if desired; here we scan recent files and let ts filter.
    matches_all: List[Dict[str, Any]] = []
    for m in methods:
        res = search_access_logs(
            s3_client=s3_client,
            source_bucket=source_bucket,
            log_bucket=log_bucket,
            credential_cache_key=credential_cache_key,
            log_prefix=log_prefix,
            method=m,
            object_key=object_key,
            limit_matches=max(20, min(200, limit)),
            metrics_bucket=metrics_bucket,
        )
        matches_all.extend(res.get("matches", []))

    # filter by time window if timestamps exist
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, min(int(hours), 720)))
    def parse_ts(s: Optional[str]) -> Optional[datetime]:
        try:
            if not s:
                return None
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    filtered = []
    for x in matches_all:
        ts = parse_ts(x.get("ts"))
        if ts and ts >= cutoff:
            filtered.append(x)
        elif ts is None:
            # keep unknown ts entries, but they might not be in window
            filtered.append(x)

    filtered.sort(key=lambda x: x.get("ts") or "", reverse=True)
    filtered = filtered[:max(1, min(int(limit), 200))]

    return {
        "source_bucket": source_bucket,
        "object_key": object_key,
        "hours": hours,
        "methods": methods,
        "count": len(filtered),
        "events": filtered,
        "note": "Use method=PUT to find upload events (and IP).",
    }


# ============================================================
# OPTIONAL SCHEDULER (snapshots every 5 minutes)
# ============================================================
@app.on_event("startup")
async def startup_scheduler():
    # Track application start time for uptime metrics
    app.state.start_time = time.time()
    logger.info("SpaceWatch application started")
    logger.info(f"Metrics tracking enabled - tracking last {MAX_LATENCY_SAMPLES} requests")
    
    # Scheduler disabled in multi-tenant mode (no global credentials)
    if not ENABLE_SCHEDULER:
        logger.info("Scheduler disabled (ENABLE_SCHEDULER=false). Snapshots will be triggered client-side.")
        return
    
    logger.warning("Scheduler is enabled but cannot run properly in multi-tenant mode (no global credentials). Consider using client-side snapshots instead.")
    # The scheduler would need per-tenant credentials to work, which we don't have globally

    async def loop():
        while True:
            STATS["scheduler_runs"] += 1
            # NOTE: This scheduler is disabled in multi-tenant mode and these calls
            # are non-functional without global credentials. Kept for legacy compatibility.
            # try:
            #     # Priority order:
            #     # 1) Explicit SCHEDULER_SOURCE_BUCKETS (doesn't require list_buckets)
            #     # 2) Bucket cache (list_buckets OR seeded known buckets)
            #     # 3) Discover from existing snapshots
            #     buckets = list(SCHEDULER_SOURCE_BUCKETS)
            #     if not buckets:
            #         buckets = sorted(list(refresh_bucket_cache()))
            #     if not buckets:
            #         ms = metrics_sources_internal(hours=720)
            #         buckets = ms.get("sources") or []
            #
            #     for b in buckets:
            #         if ACCESS_LOGS_BUCKET and b == ACCESS_LOGS_BUCKET:
            #             continue
            #         try:
            #             run_metrics_snapshot(b, "")
            #             STATS["scheduler_snapshots_ok"] += 1
            #             logger.info(f"Metrics snapshot completed for bucket: {b}")
            #         except Exception as e:
            #             STATS["scheduler_snapshots_err"] += 1
            #             logger.error(f"Metrics snapshot failed for bucket {b}: {e}")
            #             traceback.print_exc()
            # except Exception as e:
            #     STATS["scheduler_snapshots_err"] += 1
            #     logger.error(f"Scheduler error: {e}")
            #     traceback.print_exc()
            #     buckets = []
            #
            # for b in buckets:
            #     if ACCESS_LOGS_BUCKET and b == ACCESS_LOGS_BUCKET:
            #         continue
            #     try:
            #         run_metrics_snapshot(b, "")
            #         STATS["scheduler_snapshots_ok"] += 1
            #         logger.info(f"Metrics snapshot completed for bucket: {b}")
            #     except Exception as e:
            #         STATS["scheduler_snapshots_err"] += 1
            #         logger.error(f"Metrics snapshot failed for bucket {b}: {e}")
            #         traceback.print_exc()

            await asyncio.sleep(max(60, int(SNAPSHOT_EVERY_SEC)))

    asyncio.create_task(loop())


# ============================================================
# DO AGENT CALL
# ============================================================
def call_do_agent(messages: List[Dict[str, str]]) -> Tuple[str, Dict[str, Any]]:
    STATS["agent_calls"] += 1
    payload = {"messages": messages}
    headers = {"Authorization": f"Bearer {DO_AGENT_KEY}", "Content-Type": "application/json"}

    last_err = None
    for attempt in range(DO_AGENT_RETRIES + 1):
        try:
            r = httpx.post(DO_AGENT_URL, json=payload, headers=headers, timeout=DO_AGENT_TIMEOUT_SEC)
            if r.status_code >= 400:
                last_err = f"{r.status_code}: {r.text}"
            else:
                data = r.json()
                answer = data["choices"][0]["message"]["content"]
                return answer, data
        except Exception as e:
            last_err = str(e)
        time.sleep(0.25 * (attempt + 1))

    STATS["agent_failures"] += 1
    raise HTTPException(status_code=502, detail=f"Agent call failed: {last_err}")


# ============================================================
# AI TOOL FRAMEWORK
# ============================================================
def _truncate_json_for_agent(obj: Any) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    if len(s) <= AGENT_MAX_TOOL_BYTES:
        return s
    head = s[: int(AGENT_MAX_TOOL_BYTES * 0.7)]
    tail = s[-int(AGENT_MAX_TOOL_BYTES * 0.3):]
    return head + "\n...<truncated>...\n" + tail

def _parse_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Robust: extracts FIRST JSON object from the assistant message.
    - allows leading text
    - allows ``` fenced blocks
    - DOES NOT accept multiple JSON objects; we handle that separately
    """
    if not text:
        return None
    t = text.strip()

    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t).strip()
        if t.endswith("```"):
            t = t[:-3].strip()

    # quick direct parse
    try:
        return json.loads(t)
    except Exception:
        pass

    # extract first object-like region (best-effort)
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

def _count_json_objects(text: str) -> int:
    """
    Rough detection of multiple JSON objects in one message.
    We enforce ONE tool call per assistant message for reliability.
    """
    if not text:
        return 0
    # count occurrences of '{"type"' which is our contract marker
    return len(re.findall(r'\{\s*"type"\s*:', text))

def _tool_error(msg: str) -> Dict[str, Any]:
    return {"ok": False, "error": msg}

def _tool_ok(data: Any) -> Dict[str, Any]:
    return {"ok": True, "data": data}


def _build_tools(ctx: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build tool registry with context (s3_client, buckets, prefixes)"""
    s3 = ctx["s3_client"]
    credential_cache_key = ctx["credential_cache_key"]
    log_bucket = ctx.get("log_bucket")
    log_prefix = ctx.get("log_prefix", "")
    metrics_bucket = ctx.get("metrics_bucket")
    metrics_prefix = ctx.get("metrics_prefix", "")
    
    return {
        "buckets": {
            "description": "List allowed Spaces buckets. If permission fails, use metrics_sources instead.",
            "args_schema": {},
            "fn": lambda args: {"bucket_count": len(refresh_bucket_cache(s3, credential_cache_key, force=True, log_bucket=log_bucket, metrics_bucket=metrics_bucket)), "buckets": sorted(list(refresh_bucket_cache(s3, credential_cache_key, force=True, log_bucket=log_bucket, metrics_bucket=metrics_bucket)))},
        },
        "list_objects": {
            "description": "List objects in a bucket/prefix. Use max_items to keep it small.",
            "args_schema": {"bucket": "string", "prefix": "string(optional)", "max_items": "int(optional)"},
            "fn": lambda args: list_objects(s3, str(args["bucket"]), credential_cache_key, prefix=str(args.get("prefix") or ""), max_items=int(args.get("max_items") or MAX_LIST_OBJECTS_RETURN), log_bucket=log_bucket, metrics_bucket=metrics_bucket),
        },
        "recent_objects": {
            "description": "Return most recently modified objects (sorted by last_modified).",
            "args_schema": {"bucket": "string", "prefix": "string(optional)", "limit": "int(optional)"},
            "fn": lambda args: recent_objects(s3, str(args["bucket"]), credential_cache_key, prefix=str(args.get("prefix") or ""), limit=int(args.get("limit") or 10), log_bucket=log_bucket, metrics_bucket=metrics_bucket),
        },
        "storage_summary": {
            "description": "Total bytes/objects for a bucket/prefix.",
            "args_schema": {"bucket": "string", "prefix": "string(optional)"},
            "fn": lambda args: storage_summary(s3, str(args["bucket"]), credential_cache_key, prefix=str(args.get("prefix") or ""), log_bucket=log_bucket, metrics_bucket=metrics_bucket),
        },
        "top_largest": {
            "description": "Largest objects by size for a bucket/prefix.",
            "args_schema": {"bucket": "string", "prefix": "string(optional)", "limit": "int(optional)"},
            "fn": lambda args: (
                lambda bucket, prefix, limit: (
                    lambda objs: {"bucket": bucket, "prefix": prefix, "limit": limit, "objects": objs[:limit]}
                )(
                    sorted(list_objects(s3, bucket, credential_cache_key, prefix=prefix, log_bucket=log_bucket, metrics_bucket=metrics_bucket), key=lambda x: x["size_bytes"], reverse=True)
                )
            )(str(args["bucket"]), str(args.get("prefix") or ""), max(1, min(int(args.get("limit") or 10), MAX_TOP_LARGEST))),
        },

        "snapshot": {
            "description": "Write a metrics snapshot for a source bucket into metrics_bucket (storage + request stats from logs).",
            "args_schema": {"source_bucket": "string", "source_prefix": "string(optional)"},
            "fn": lambda args: run_metrics_snapshot(s3, str(args["source_bucket"]), credential_cache_key, str(args.get("source_prefix") or ""), log_bucket, log_prefix, metrics_bucket, metrics_prefix),
        },
        "metrics_sources": {
            "description": "Discover source buckets by scanning stored snapshots in metrics_bucket/metrics_prefix.",
            "args_schema": {"hours": "int(optional)"},
            "fn": lambda args: metrics_sources_internal(s3, metrics_bucket, credential_cache_key, metrics_prefix, hours=int(args.get("hours") or 720), log_bucket=log_bucket),
        },
        "metrics_series": {
            "description": "Get time series snapshot points for a source bucket/prefix.",
            "args_schema": {"source_bucket": "string", "source_prefix": "string(optional)", "hours": "int(optional)", "limit": "int(optional)"},
            "fn": lambda args: metrics_series_internal(
                s3,
                source_bucket=str(args["source_bucket"]),
                metrics_bucket=metrics_bucket,
                credential_cache_key=credential_cache_key,
                metrics_prefix=metrics_prefix,
                source_prefix=str(args.get("source_prefix") or ""),
                hours=int(args.get("hours") or 720),
                limit=int(args.get("limit") or 500),
                log_bucket=log_bucket,
            ),
        },

        "list_access_logs": {
            "description": "List log objects (.log/.log.gz) for a source bucket in log_bucket.",
            "args_schema": {"source_bucket": "string", "date_yyyy_mm_dd": "string(optional)", "max_items": "int(optional)"},
            "fn": lambda args: list_access_log_objects_for_source(
                s3,
                str(args["source_bucket"]),
                log_bucket,
                credential_cache_key,
                log_prefix,
                date_yyyy_mm_dd=args.get("date_yyyy_mm_dd"),
                max_items=int(args.get("max_items") or 50),
                metrics_bucket=metrics_bucket,
            ),
        },
        "read_log": {
            "description": "Read tail of a log object (supports .gz). Provide bucket+key.",
            "args_schema": {"bucket": "string", "key": "string", "tail_lines": "int(optional)"},
            "fn": lambda args: read_log_object(s3, str(args["bucket"]), str(args["key"]), credential_cache_key, int(args.get("tail_lines") or 200), log_bucket=log_bucket, metrics_bucket=metrics_bucket),
        },
        "search_logs": {
            "description": "Search access logs for a bucket with filters: method/object_key/status_prefix/contains (supports .gz).",
            "args_schema": {
                "source_bucket": "string",
                "contains": "string(optional)",
                "method": "string(optional)",
                "object_key": "string(optional)",
                "status_prefix": "string(optional)",
                "date_yyyy_mm_dd": "string(optional)",
                "limit_matches": "int(optional)",
            },
            "fn": lambda args: search_access_logs(
                s3,
                source_bucket=str(args["source_bucket"]),
                log_bucket=log_bucket,
                credential_cache_key=credential_cache_key,
                log_prefix=log_prefix,
                contains=args.get("contains"),
                method=args.get("method"),
                object_key=args.get("object_key"),
                status_prefix=args.get("status_prefix"),
                date_yyyy_mm_dd=args.get("date_yyyy_mm_dd"),
                limit_matches=int(args.get("limit_matches") or 20),
                metrics_bucket=metrics_bucket,
            ),
        },
        "object_audit": {
            "description": "CloudTrail-ish event timeline for an object (find upload IP via PUT).",
            "args_schema": {
                "source_bucket": "string",
                "object_key": "string",
                "hours": "int(optional)",
                "limit": "int(optional)",
                "methods": "list(optional)",
            },
            "fn": lambda args: object_audit_timeline(
                s3,
                source_bucket=str(args["source_bucket"]),
                object_key=str(args["object_key"]),
                log_bucket=log_bucket,
                credential_cache_key=credential_cache_key,
                log_prefix=log_prefix,
                hours=int(args.get("hours") or 168),
                limit=int(args.get("limit") or 50),
                methods=args.get("methods") if isinstance(args.get("methods"), list) else None,
                metrics_bucket=metrics_bucket,
            ),
        },
    }


def _execute_tool(tool: str, args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    tools = _build_tools(ctx)
    spec = tools.get(tool)
    if not spec:
        return _tool_error(f"Unknown tool: {tool}")
    try:
        out = spec["fn"](args or {})
        # unify shape
        if isinstance(out, dict) and "ok" in out and ("data" in out or "error" in out):
            return out
        return _tool_ok(out)
    except Exception as e:
        return _tool_error(f"{type(e).__name__}: {e}")


def _build_agent_system(ctx: Dict[str, Any]) -> str:
    """Build AGENT_SYSTEM prompt with tools from context"""
    tools = _build_tools(ctx)
    return f"""
You are SpaceWatch, an AI observability assistant for object storage (DigitalOcean Spaces / S3-style) and access logs.
You have tool access through this backend. YOU are the driver. You must decide which tools to call.

Hard rules:
- You MUST NOT ask the user to choose buckets unless it is truly impossible to proceed.
- Use tools to discover buckets: prefer metrics_sources(); if buckets() works, you can use it too.
- If user asks "last uploaded file", use recent_objects() (it sorts by last_modified).
- If user asks "from which IP uploaded", use object_audit(methods=["PUT"]) or search_logs(method="PUT", object_key=...).
- Output EXACTLY ONE JSON object and NOTHING ELSE (no extra text).
- NEVER output more than one tool call per message.

Response format:
- To call a tool:
  {{"type":"tool_call","tool":"<tool_name>","args":{{...}}}}
- To answer:
  {{"type":"final","answer":"<plain English answer>"}}.

Available tools and schemas:
{json.dumps({k: {"description": v["description"], "args_schema": v["args_schema"]} for k, v in tools.items()}, indent=2)}
""".strip()


# ============================================================
# CHAT (AI-driven tool loop)
# ============================================================
@app.post("/chat")
def chat(req: ChatRequest, request: Request, x_api_key: Optional[str] = Header(None), x_session_id: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    rate_limit(client_ip(request))
    STATS["chat_requests"] += 1

    user_msg = (req.message or "").strip()
    if not user_msg:
        raise HTTPException(status_code=400, detail="Empty message")

    # Create S3 client from request credentials
    s3_client = create_s3_client(req.spaces_key.get_secret_value(), req.spaces_secret.get_secret_value(), req.region, req.endpoint)
    
    # Generate credential cache key for BYOC-safe caching
    credential_cache_key = get_credential_cache_key(req.spaces_key.get_secret_value(), req.region, req.endpoint)
    
    # Build context for tools
    tool_ctx = {
        "s3_client": s3_client,
        "credential_cache_key": credential_cache_key,
        "log_bucket": req.log_bucket,
        "log_prefix": req.log_prefix or "",
        "metrics_bucket": req.metrics_bucket,
        "metrics_prefix": req.metrics_prefix or "spacewatch-metrics/",
    }

    mkey = memory_key(request, x_session_id, credential_cache_key)
    mem = get_memory(mkey)

    context = {
        "now_utc": datetime.now(timezone.utc).isoformat(),
        "spaces_endpoint": req.endpoint or DEFAULT_SPACES_ENDPOINT,
        "access_logs_bucket": req.log_bucket or "not_configured",
        "access_logs_prefix": normalize_prefix(req.log_prefix or ""),
        "metrics_bucket": req.metrics_bucket or "not_configured",
        "metrics_prefix": normalize_prefix(req.metrics_prefix or "spacewatch-metrics/"),
        "bucket_cache_count": len(refresh_bucket_cache(s3_client, credential_cache_key, log_bucket=req.log_bucket, metrics_bucket=req.metrics_bucket)),
        "fallback_buckets_configured": bool(FALLBACK_BUCKETS),
        "last_bucket_context": mem.last_bucket if mem else None,
        "last_tool_context": mem.last_tool if mem else None,
        "notes": [
            "Access logs may contain URL-encoded keys (spaces become %20).",
            "S3 object listings are NOT ordered by last_modified; use recent_objects().",
        ],
    }

    agent_system = _build_agent_system(tool_ctx)
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": agent_system},
        {"role": "user", "content": json.dumps({"context": context, "user_question": user_msg}, indent=2)},
    ]

    last_bucket_used: Optional[str] = None
    last_tool_used: Optional[str] = None

    for _step in range(AGENT_MAX_STEPS):
        content, raw = call_do_agent(messages)

        # Enforce: one JSON object only. If model outputs multiple, force retry.
        if _count_json_objects(content) > 1:
            messages.append({
                "role": "user",
                "content": json.dumps({
                    "error": "You output more than one JSON object/tool call. Output EXACTLY ONE JSON object only.",
                    "required_format": {"type": "tool_call|final", "tool": "...", "args": {}, "answer": "..."}
                })
            })
            continue

        parsed = _parse_first_json_object(content)
        if not parsed:
            messages.append({
                "role": "user",
                "content": json.dumps({
                    "error": "You did not output valid JSON. Output EXACTLY ONE JSON object only.",
                    "required_format": {"type": "tool_call|final", "tool": "...", "args": {}, "answer": "..."}
                })
            })
            continue

        if parsed.get("type") == "final":
            ans = str(parsed.get("answer") or "").strip()
            update_memory(mkey, bucket=last_bucket_used, tool=last_tool_used or "chat")
            return {"answer": ans, "tool_used": last_tool_used, "bucket_used": last_bucket_used, "raw": raw}

        if parsed.get("type") != "tool_call":
            messages.append({
                "role": "user",
                "content": json.dumps({"error": "Invalid type. Use type=tool_call or type=final only."})
            })
            continue

        tool = str(parsed.get("tool") or "").strip()
        args = parsed.get("args") or {}
        if not isinstance(args, dict):
            args = {}

        # track context for follow-ups
        for k in ("bucket", "source_bucket"):
            if k in args and isinstance(args[k], str) and args[k].strip():
                last_bucket_used = args[k].strip()
                break
        last_tool_used = tool

        STATS["tool_requests"] += 1
        tool_result = _execute_tool(tool, args, tool_ctx)

        messages.append({
            "role": "user",
            "content": json.dumps({
                "tool_result": {
                    "tool": tool,
                    "args": args,
                    "result": json.loads(_truncate_json_for_agent(tool_result)),
                },
                "instruction": "Use this tool_result. Decide next tool_call or return final. Output ONE JSON object only."
            })
        })

    update_memory(mkey, bucket=last_bucket_used, tool=last_tool_used or "chat")
    return {
        "answer": f"I hit the maximum tool steps ({AGENT_MAX_STEPS}) for this request. Try narrowing the question (one bucket or one object).",
        "tool_used": last_tool_used,
        "bucket_used": last_bucket_used,
    }


# ============================================================
# HEALTH / STATS / OPTIONAL ENDPOINTS
# ============================================================
@app.get("/health")
def health():
    storage_metrics = get_storage_metrics()
    
    return {
        "ok": True,
        "status": "healthy",
        "uptime_seconds": time.time() - app.state.start_time if hasattr(app.state, 'start_time') else 0,
        "spaces_endpoint": DEFAULT_SPACES_ENDPOINT,
        "spaces_region": DEFAULT_SPACES_REGION,
        "api_key_protection": bool(APP_API_KEY),
        "bucket_cache_credentials_count": len(_BUCKET_CACHE_BY_CREDENTIAL),
        "fallback_buckets_enabled": bool(FALLBACK_BUCKETS),
        "agent": {"max_steps": AGENT_MAX_STEPS, "max_tool_bytes": AGENT_MAX_TOOL_BYTES},
        "scheduler": {"enabled": ENABLE_SCHEDULER, "every_sec": SNAPSHOT_EVERY_SEC},
        "storage_metrics": storage_metrics,
    }

@app.get("/metrics/operations")
def storage_operations_metrics(x_api_key: Optional[str] = Header(None)):
    """
    Storage operation metrics endpoint (S3/Azure Blob style).
    Returns detailed storage operation analytics and performance metrics.
    """
    require_api_key(x_api_key)
    
    storage_metrics = get_storage_metrics()
    
    # Calculate additional analytics
    now = time.time()
    recent_1h = [op for op in STORAGE_OPERATION_METRICS if now - op.start_time <= 3600]
    recent_24h = [op for op in STORAGE_OPERATION_METRICS if now - op.start_time <= 86400]
    
    # Bucket-level analytics
    bucket_stats = defaultdict(lambda: {"operations": 0, "errors": 0, "bytes": 0, "latencies": []})
    for op in recent_1h:
        if op.bucket:
            stats = bucket_stats[op.bucket]
            stats["operations"] += 1
            if op.status_code >= 400:
                stats["errors"] += 1
            stats["bytes"] += op.bytes_transferred
            stats["latencies"].append(op.duration_ms)
    
    bucket_analytics = {}
    for bucket, stats in bucket_stats.items():
        bucket_analytics[bucket] = {
            "operation_count": stats["operations"],
            "error_count": stats["errors"],
            "error_rate": round(stats["errors"] / stats["operations"] * 100, 2) if stats["operations"] > 0 else 0,
            "bytes_transferred": stats["bytes"],
            "avg_latency_ms": round(sum(stats["latencies"]) / len(stats["latencies"]), 2) if stats["latencies"] else 0
        }
    
    # Time-based trends
    hour_trend = len(recent_1h)
    day_trend = len(recent_24h)
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "current_5m": storage_metrics,
        "trends": {
            "operations_last_1h": hour_trend,
            "operations_last_24h": day_trend,
        },
        "bucket_analytics": bucket_analytics,
        "total_operations_tracked": len(STORAGE_OPERATION_METRICS)
    }

@app.get("/logs/operations")
def operation_logs(
    operation_type: Optional[str] = None,
    bucket: Optional[str] = None,
    limit: int = 100,
    min_duration_ms: Optional[float] = None,
    x_api_key: Optional[str] = Header(None)
):
    """
    Get recent storage operation logs (S3 access logs style).
    Filter by operation type, bucket, or slow operations.
    """
    require_api_key(x_api_key)
    
    limit = max(1, min(limit, 1000))
    
    # Filter operations
    filtered_ops = STORAGE_OPERATION_METRICS[:]
    
    if operation_type:
        filtered_ops = [op for op in filtered_ops if op.operation_type == operation_type.upper()]
    
    if bucket:
        filtered_ops = [op for op in filtered_ops if op.bucket == bucket]
    
    if min_duration_ms:
        filtered_ops = [op for op in filtered_ops if op.duration_ms >= min_duration_ms]
    
    # Sort by most recent first
    filtered_ops.sort(key=lambda x: x.start_time, reverse=True)
    filtered_ops = filtered_ops[:limit]
    
    # Format logs
    logs = []
    for op in filtered_ops:
        logs.append({
            "timestamp": datetime.fromtimestamp(op.start_time, tz=timezone.utc).isoformat(),
            "operation_type": op.operation_type,
            "method": op.method,
            "endpoint": op.endpoint,
            "bucket": op.bucket,
            "status_code": op.status_code,
            "duration_ms": round(op.duration_ms, 2),
            "bytes_transferred": op.bytes_transferred,
            "success": op.status_code < 400
        })
    
    return {
        "count": len(logs),
        "logs": logs,
        "filters": {
            "operation_type": operation_type,
            "bucket": bucket,
            "min_duration_ms": min_duration_ms
        },
        "note": "Storage operation logs in S3 access logs style format"
    }

@app.get("/stats")
def stats(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    storage_metrics = get_storage_metrics()
    return {**STATS, **storage_metrics}

@app.post("/metrics/snapshot")
def metrics_snapshot(
    source_bucket: str,
    source_prefix: str = "",
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    log_prefix: Optional[str] = Header(None, alias="X-Log-Prefix"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not log_bucket or not metrics_bucket:
        raise HTTPException(status_code=400, detail="X-Log-Bucket and X-Metrics-Bucket headers required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    return run_metrics_snapshot(s3_client, source_bucket, credential_cache_key, source_prefix, log_bucket, log_prefix or "", metrics_bucket, metrics_prefix or "")

@app.get("/metrics/top-ips")
def get_top_ips(
    source_bucket: str,
    date_yyyy_mm_dd: Optional[str] = None,
    limit: int = 20,
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    log_prefix: Optional[str] = Header(None, alias="X-Log-Prefix"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    """
    Return top IPs data as JSON for client-side Chart.js rendering.
    """
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not log_bucket:
        raise HTTPException(status_code=400, detail="X-Log-Bucket header required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    
    # Use log search and count IPs
    res = search_access_logs(
        s3_client=s3_client,
        source_bucket=source_bucket,
        log_bucket=log_bucket,
        credential_cache_key=credential_cache_key,
        log_prefix=log_prefix or "",
        date_yyyy_mm_dd=date_yyyy_mm_dd,
        limit_matches=2000,
        metrics_bucket=metrics_bucket,
    )
    ip_counts = Counter()
    for m in res.get("matches", []):
        if m.get("ip"):
            ip_counts[m["ip"]] += 1

    top = ip_counts.most_common(max(1, min(int(limit), 50)))
    
    return {
        "top_ips": [{"ip": ip, "count": count} for ip, count in top],
        "total_ips": len(ip_counts),
        "source_bucket": source_bucket,
        "date": date_yyyy_mm_dd
    }

@app.get("/plots/top-ips.png")
def plot_top_ips_png(
    source_bucket: str,
    date_yyyy_mm_dd: Optional[str] = None,
    limit: int = 20,
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    log_prefix: Optional[str] = Header(None, alias="X-Log-Prefix"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    x_api_key: Optional[str] = Header(None),
    request: Request = None,
):
    require_api_key(x_api_key)
    if request:
        rate_limit(client_ip(request))
    
    if not log_bucket:
        raise HTTPException(status_code=400, detail="X-Log-Bucket header required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    credential_cache_key = get_credential_cache_key(spaces_key, region, endpoint)
    
    # -------------------------
    # CACHE CHECK (per-credential for BYOC safety)
    # -------------------------
    cache_key = (credential_cache_key, source_bucket, date_yyyy_mm_dd or "", int(limit))
    now = time.time()
    cached = TOP_IPS_CACHE.get(cache_key)
    if cached and (now - cached[0]) < TOP_IPS_TTL:
        return Response(
            content=cached[1],
            media_type="image/png"
        )
    
    # Use log search and count IPs
    res = search_access_logs(
        s3_client=s3_client,
        source_bucket=source_bucket,
        log_bucket=log_bucket,
        credential_cache_key=credential_cache_key,
        log_prefix=log_prefix or "",
        date_yyyy_mm_dd=date_yyyy_mm_dd,
        limit_matches=2000,
        metrics_bucket=metrics_bucket,
    )
    ip_counts = Counter()
    for m in res.get("matches", []):
        if m.get("ip"):
            ip_counts[m["ip"]] += 1

    top = ip_counts.most_common(max(1, min(int(limit), 50)))
    if not top:
        fig = plt.figure(figsize=(8, 2.5))
        plt.text(0.5, 0.5, "No IP data found", ha="center", va="center")
        plt.axis("off")
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
        plt.close(fig)
        png_bytes = buf.getvalue()
        TOP_IPS_CACHE[cache_key] = (now, png_bytes)
    
        return Response(content=png_bytes, media_type="image/png")

    ips = [x[0] for x in top]
    counts = [x[1] for x in top]

    fig = plt.figure(figsize=(10, 4.5))
    plt.bar(range(len(ips)), counts)
    plt.xticks(range(len(ips)), ips, rotation=45, ha="right")
    plt.ylabel("Requests (count)")
    plt.title(f"Top IPs for {source_bucket}" + (f" on {date_yyyy_mm_dd}" if date_yyyy_mm_dd else ""))
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140)
    plt.close(fig)
    
    png_bytes = buf.getvalue()
    TOP_IPS_CACHE[cache_key] = (now, png_bytes)
    return Response(content=png_bytes, media_type="image/png")

@app.post("/validate-credentials")
def validate_credentials(
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    x_api_key: Optional[str] = Header(None),
):
    """
    Validate Spaces credentials by attempting to list buckets.
    Returns success if credentials are valid.
    """
    require_api_key(x_api_key)
    
    try:
        s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
        # Try to list buckets to validate credentials
        buckets = refresh_bucket_cache(s3_client, force=True, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
        
        return {
            "valid": True,
            "bucket_count": len(buckets),
            "buckets": sorted(list(buckets))
        }
    except Exception as e:
        logger.error(f"Credential validation failed: {e}")
        raise HTTPException(status_code=401, detail=f"Invalid credentials: {str(e)}")

@app.post("/trigger-snapshot-all")
def trigger_snapshot_all(
    spaces_key: str = Header(..., alias="X-Spaces-Key"),
    spaces_secret: str = Header(..., alias="X-Spaces-Secret"),
    region: Optional[str] = Header(None, alias="X-Region"),
    endpoint: Optional[str] = Header(None, alias="X-Endpoint"),
    log_bucket: Optional[str] = Header(None, alias="X-Log-Bucket"),
    log_prefix: Optional[str] = Header(None, alias="X-Log-Prefix"),
    metrics_bucket: Optional[str] = Header(None, alias="X-Metrics-Bucket"),
    metrics_prefix: Optional[str] = Header(None, alias="X-Metrics-Prefix"),
    x_api_key: Optional[str] = Header(None),
):
    """
    Trigger metrics snapshots for all discovered buckets.
    This is called automatically after credential validation.
    """
    require_api_key(x_api_key)
    
    if not log_bucket:
        raise HTTPException(status_code=400, detail="X-Log-Bucket header required")
    if not metrics_bucket:
        raise HTTPException(status_code=400, detail="X-Metrics-Bucket header required")
    
    s3_client = create_s3_client(spaces_key, spaces_secret, region, endpoint)
    
    # Get all buckets
    buckets = refresh_bucket_cache(s3_client, force=True, log_bucket=log_bucket, metrics_bucket=metrics_bucket)
    
    results = []
    errors = []
    
    # Skip log and metrics buckets
    skip_buckets = set()
    if log_bucket:
        skip_buckets.add(log_bucket)
    if metrics_bucket:
        skip_buckets.add(metrics_bucket)
    
    for bucket in buckets:
        if bucket in skip_buckets:
            continue
        
        try:
            result = run_metrics_snapshot(
                s3_client,
                source_bucket=bucket,
                source_prefix="",
                log_bucket=log_bucket,
                log_prefix=log_prefix if log_prefix else "",
                metrics_bucket=metrics_bucket,
                metrics_prefix=metrics_prefix if metrics_prefix else "spacewatch-metrics/"
            )
            results.append({
                "bucket": bucket,
                "status": "success",
                "metrics_key": result.get("metrics_key")
            })
        except Exception as e:
            logger.error(f"Snapshot failed for bucket {bucket}: {e}")
            errors.append({
                "bucket": bucket,
                "status": "error",
                "error": str(e)
            })
    
    return {
        "snapshots_created": len(results),
        "snapshots_failed": len(errors),
        "results": results,
        "errors": errors
    }
