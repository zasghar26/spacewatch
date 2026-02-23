# SpaceWatch - AI-Driven Observability Backend

SpaceWatch is an AI-driven observability backend for DigitalOcean Spaces (S3-compatible) object storage and access logs, with **AWS S3 and Azure Blob Storage style metrics and logging**.

## Enhanced Features

### Storage Metrics (S3/Azure Blob Style)
- **Real-time operation tracking**: Monitor storage operations with latency percentiles (P50, P95, P99)
- **Operation breakdown**: Track GET, PUT, DELETE, HEAD, and LIST operations separately
- **Error rate monitoring**: Track success/failure rates for storage operations
- **Data transfer metrics**: Monitor ingress/egress bandwidth in real-time
- **Structured logging**: S3-style operation logs with correlation IDs and detailed metrics

### Advanced Monitoring
- **Latency analytics**: Percentile calculations for performance monitoring
- **Slow operation detection**: Automatic logging of operations exceeding 5 seconds
- **Operation-level insights**: Per-bucket and per-operation-type analytics
- **Storage health dashboard**: Real-time health status similar to AWS CloudWatch

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

**Option A: Interactive Setup (Recommended)**

Run the interactive setup wizard to configure SpaceWatch:

```bash
python setup.py
```

The wizard will prompt you for:
- **DO_AGENT_URL** - OpenAI-compatible chat completions endpoint (required)
- **DO_AGENT_KEY** - API key for the AI agent (required)
- **APP_API_KEY** - API key to protect admin endpoints (optional, recommended for production)
- **SPACES_REGION** - Default DigitalOcean Spaces region (optional, default: sgp1)
- **SPACES_ENDPOINT** - Default Spaces endpoint URL (optional, auto-generated from region)

**Option B: Manual Configuration**

Copy the sample environment file and configure your settings:

```bash
cp sample.env .env
```

Edit `.env` and set your actual values:

```bash
# AI Agent (OpenAI-compatible) - REQUIRED
DO_AGENT_URL=https://your-agent-host/v1/chat/completions
DO_AGENT_KEY=your_actual_agent_api_key

# App Security (optional, for admin endpoints)
APP_API_KEY=your_api_key_here

# Default Region (optional)
SPACES_REGION=sgp1
SPACES_ENDPOINT=https://sgp1.digitaloceanspaces.com
```

**Note:** DigitalOcean Spaces credentials are now provided per-request, not globally configured. See the Multi-Tenant Usage section below for details.

### 3. Authentication & Security

SpaceWatch has two types of endpoints:

**User Endpoints** - These endpoints allow users to access their own Spaces buckets using their Spaces credentials. No API key required:
- `/chat` - AI-driven chat interface
- `/tools/*` - Bucket and object management tools
- `/metrics/*` - Metrics and monitoring for user buckets
- `/validate-credentials` - Credential validation

**Admin Endpoints** - These endpoints provide system-wide monitoring and require the `X-API-Key` header (set via `APP_API_KEY` environment variable):
- `/admin/api/mission-control` - System health dashboard
- `/admin/api/timeseries` - System-wide timeseries data
- `/admin/api/events` - System events log
- `/metrics/operations` - Storage operation metrics
- `/logs/operations` - Operation logs
- `/stats` - System statistics

### 4. Run the Application

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Or for development with auto-reload:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Multi-Tenant Usage

SpaceWatch now supports multi-tenant usage where each request provides its own DigitalOcean Spaces credentials. This allows multiple users to access their own Spaces resources securely without sharing credentials.

### Chat Endpoint

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Show me my largest files",
    "spaces_key": "your_spaces_access_key",
    "spaces_secret": "your_spaces_secret_key",
    "log_bucket": "my-access-logs",
    "log_prefix": "spaces-logs/",
    "metrics_bucket": "my-metrics",
    "metrics_prefix": "spacewatch-metrics/"
  }'
```

### Tool Endpoints

All tool endpoints now accept credentials via headers:

```bash
curl -X GET "http://localhost:8000/tools/buckets" \
  -H "X-Spaces-Key: your_spaces_access_key" \
  -H "X-Spaces-Secret: your_spaces_secret_key"
```

```bash
curl -X GET "http://localhost:8000/tools/list-all?bucket=my-bucket" \
  -H "X-Spaces-Key: your_spaces_access_key" \
  -H "X-Spaces-Secret: your_spaces_secret_key" \
  -H "X-Log-Bucket: my-access-logs" \
  -H "X-Metrics-Bucket: my-metrics"
```

### Required Headers

- `X-Spaces-Key` - Your DigitalOcean Spaces access key
- `X-Spaces-Secret` - Your DigitalOcean Spaces secret key

### Optional Headers

- `X-Log-Bucket` - Bucket containing access logs (required for log-related queries)
- `X-Log-Prefix` - Prefix for access logs (default: "")
- `X-Metrics-Bucket` - Bucket for storing metrics (required for metrics operations)
- `X-Metrics-Prefix` - Prefix for metrics (default: "spacewatch-metrics/")
- `X-Region` - Spaces region (default: from SPACES_REGION env var)
- `X-Endpoint` - Spaces endpoint URL (default: from SPACES_ENDPOINT env var)

## Environment Variables

The application automatically loads environment variables from a `.env` file in the project root directory.

### Required Variables

- `DO_AGENT_URL` - OpenAI-compatible chat completions endpoint
- `DO_AGENT_KEY` - API key for the AI agent

### Optional Variables

- `APP_API_KEY` - API key to protect admin endpoints (recommended for production)
- `SPACES_REGION` - Default DigitalOcean Spaces region (default: "sgp1")
- `SPACES_ENDPOINT` - Default Spaces endpoint URL

See `sample.env` for a complete list of optional configuration variables.

## Features

- **Multi-tenant support** - Each request uses its own credentials
- **AI-driven observability assistant**
- **Real-time storage operation metrics** (S3/Azure Blob style)
  - Operation latency tracking with percentiles (P50, P95, P99)
  - Operation breakdown by type (GET, PUT, DELETE, HEAD, LIST)
  - Error rate monitoring and alerting
  - Data transfer rate analytics
- **Structured logging** with operation-level details
- **Bucket and object management**
- **Access log analysis**
- **Metrics snapshots and time series**
- **Storage analytics** with hot/cold data patterns
- **IP tracking and visualization**

## API Endpoints

### Core Endpoints
- `/` - Home page with dashboard
- `/chat` - AI chat interface
- `/health` - Enhanced health check with storage metrics

### Storage Metrics (New!)
- `/metrics/operations` - Detailed storage operation analytics
- `/logs/operations` - Storage operation logs (S3 access logs style)
- `/stats` - Application statistics with storage metrics

### Data Endpoints
- `/metrics/sources` - List metrics sources
- `/metrics/series` - Get time series data
- `/metrics/aggregate-series` - Aggregated metrics across buckets
- `/tools/*` - Various tool endpoints

See the application code for detailed API documentation.
