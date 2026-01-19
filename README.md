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

Copy the sample environment file and configure your credentials:

```bash
cp sample.env .env
```

Edit `.env` and set your actual values:

```bash
# DigitalOcean Spaces
SPACES_REGION=sgp1
SPACES_ENDPOINT=https://sgp1.digitaloceanspaces.com
SPACES_KEY=your_actual_spaces_access_key
SPACES_SECRET=your_actual_spaces_secret_key

# AI Agent (OpenAI-compatible)
DO_AGENT_URL=https://your-agent-host/v1/chat/completions
DO_AGENT_KEY=your_actual_agent_api_key

# Access Logs + Metrics
ACCESS_LOGS_BUCKET=my-access-logs
ACCESS_LOGS_ROOT_PREFIX=spaces-logs/

METRICS_BUCKET=my-access-logs
METRICS_PREFIX=spacewatch-metrics/

# App Security (optional but recommended)
APP_API_KEY=your_api_key_here
```

### 3. Run the Application

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Or for development with auto-reload:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Environment Variables

The application automatically loads environment variables from a `.env` file in the project root directory.

### Required Variables

- `SPACES_KEY` - DigitalOcean Spaces access key
- `SPACES_SECRET` - DigitalOcean Spaces secret key
- `DO_AGENT_URL` - OpenAI-compatible chat completions endpoint
- `DO_AGENT_KEY` - API key for the AI agent

### Optional Variables

See `sample.env` for a complete list of optional configuration variables.

## Features

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
