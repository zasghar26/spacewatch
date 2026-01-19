# Storage Metrics and Logging Guide

This guide explains the enhanced storage metrics and logging features in SpaceWatch, designed to provide AWS S3 and Azure Blob Storage style observability.

## Overview

SpaceWatch now tracks and displays real-time storage operation metrics similar to AWS S3 and Azure Blob Storage monitoring dashboards. All metrics are focused on **storage operations only** (not system-level metrics like CPU or memory).

## Features

### 1. Real-Time Storage Operation Metrics

The application automatically tracks every storage operation with:

- **Operation Type**: Categorized as LIST_BUCKETS, LIST_OBJECTS, ANALYZE_STORAGE, QUERY_METRICS, QUERY_LOGS, AI_QUERY
- **Latency Tracking**: P50, P95, P99 percentiles for performance monitoring
- **Error Rates**: Success/failure rates for each operation type
- **Data Transfer**: Bytes transferred per operation
- **Request Tracing**: Each request gets a unique ID for correlation

### 2. Storage Operation Dashboard

The main dashboard displays:

- **Operations/sec**: Current operation rate
- **Total Operations**: Count of operations in last 5 minutes
- **Error Rate**: Percentage of failed operations
- **Data Transfer/sec**: Bandwidth utilization
- **Latency Percentiles**: P50, P95, P99 response times
- **Operation Breakdown**: Per-operation-type statistics

### 3. Structured Logging (S3-Style)

Every storage operation is logged with structured information:

```
2024-01-15 10:30:45 [INFO] spacewatch - STORAGE_OP: LIST_OBJECTS | GET /tools/list-all | status=200 | duration=45.23ms | bytes=2048 | bucket=my-bucket
```

Slow operations (>5 seconds) are automatically flagged:

```
2024-01-15 10:35:12 [WARNING] spacewatch - SLOW_OPERATION: QUERY_LOGS /logs/operations - 5234.56ms - bucket=logs-bucket status=200
```

## API Endpoints

### `/health` - Enhanced Health Check

Returns comprehensive storage health metrics:

```json
{
  "ok": true,
  "status": "healthy",
  "uptime_seconds": 3600,
  "storage_metrics": {
    "total_operations_5m": 150,
    "error_operations_5m": 2,
    "error_rate_percent": 1.33,
    "operations_per_second": 0.5,
    "total_bytes_transferred_5m": 1048576,
    "bytes_per_second": 3495.25,
    "latency_p50_ms": 45.2,
    "latency_p95_ms": 234.5,
    "latency_p99_ms": 567.8,
    "avg_latency_ms": 78.3,
    "operation_breakdown": {
      "LIST_OBJECTS": {
        "request_count": 50,
        "error_count": 1,
        "error_rate_percent": 2.0,
        "bytes_transferred": 524288,
        "p50": 42.1,
        "p95": 156.7,
        "p99": 289.4
      },
      "QUERY_METRICS": {
        "request_count": 100,
        "error_count": 1,
        "error_rate_percent": 1.0,
        "bytes_transferred": 524288,
        "p50": 48.5,
        "p95": 298.2,
        "p99": 678.9
      }
    }
  }
}
```

### `/metrics/operations` - Detailed Storage Analytics

Returns detailed storage operation analytics with bucket-level breakdown:

```bash
curl -H "X-API-Key: your_key" http://localhost:8000/metrics/operations
```

Response includes:
- Current 5-minute metrics
- 1-hour and 24-hour trends
- Per-bucket analytics
- Total operations tracked

### `/logs/operations` - Storage Operation Logs

Query storage operation logs (S3 access logs style):

```bash
# Get all recent operations
curl -H "X-API-Key: your_key" http://localhost:8000/logs/operations

# Filter by operation type
curl -H "X-API-Key: your_key" http://localhost:8000/logs/operations?operation_type=LIST_OBJECTS

# Filter by bucket
curl -H "X-API-Key: your_key" http://localhost:8000/logs/operations?bucket=my-bucket

# Find slow operations (>1000ms)
curl -H "X-API-Key: your_key" http://localhost:8000/logs/operations?min_duration_ms=1000
```

## Dashboard Usage

### Auto-Refresh

The storage health metrics panel automatically refreshes every 30 seconds to provide real-time monitoring.

### Manual Refresh

Click the "Refresh Metrics" button to immediately update all storage operation metrics.

### Health Status

The health indicator shows:
- **Healthy** (green): Error rate < 5%
- **Degraded** (yellow): Error rate 5-10%
- **Unhealthy** (red): Error rate > 10%

## Monitoring Best Practices

### 1. Watch for Slow Operations

Operations taking >5 seconds are logged as warnings. Monitor these logs to identify performance issues:

```bash
# In production, pipe logs to a file or log aggregation service
uvicorn main:app --log-level info | grep SLOW_OPERATION
```

### 2. Track Error Rates

Monitor the error rate percentage on the dashboard. Sustained rates above 5% indicate issues with:
- Bucket permissions
- Network connectivity
- Storage service availability
- Invalid API calls

### 3. Monitor Latency Percentiles

- **P50 (median)**: Typical user experience
- **P95**: Experience for 95% of users
- **P99**: Worst-case latency (catches outliers)

If P99 > 5x P50, investigate for:
- Network issues
- Large object transfers
- API rate limiting

### 4. Analyze Operation Breakdown

The operation breakdown shows which operation types are most common. Use this to:
- Optimize frequently-used operations
- Identify unusual access patterns
- Detect potential abuse or misconfiguration

## Integration with External Monitoring

### Custom Headers

Every response includes monitoring headers:

```
X-Request-Duration-Ms: 45.23
X-Request-Id: req-1705318245123
X-Operation-Type: LIST_OBJECTS
```

Use these for:
- Client-side performance monitoring
- Request correlation across systems
- Debugging specific operations

### Structured Logs

All logs use a structured format compatible with log aggregation services:

```
timestamp [level] logger - message | key=value | key=value
```

Export to:
- AWS CloudWatch Logs
- Azure Monitor
- Elasticsearch/Logstash/Kibana (ELK)
- Splunk
- Datadog

## Example Queries for AI Assistant

Try these queries in the AI chat to leverage storage metrics:

1. "Show me storage summary for [bucket-name]"
2. "What are the top 10 largest objects in [bucket-name]?"
3. "List recent objects uploaded to [bucket-name]"
4. "Search access logs for errors in [bucket-name]"
5. "Show me objects modified in the last 24 hours"
6. "Which IP uploaded the most recent file to [bucket-name]?"

## Troubleshooting

### No Metrics Showing

If the storage metrics panel shows all zeros:
1. Ensure you've made at least one API call to the backend
2. Check that the application has been running for at least a few seconds
3. Verify the `/health` endpoint is accessible

### High Error Rates

If you see consistently high error rates:
1. Check the `/logs/operations` endpoint for specific error details
2. Verify your DigitalOcean Spaces credentials
3. Ensure bucket names and permissions are correct
4. Check network connectivity to DigitalOcean

### Slow Operations

If seeing many slow operations:
1. Check network latency to DigitalOcean Spaces
2. Review object sizes being transferred
3. Consider implementing caching for frequently accessed data
4. Check if rate limits are being hit

## Advanced Configuration

### Adjust Metrics Retention

By default, the application tracks the last 10,000 operations. To adjust:

```python
# In main.py
MAX_STORAGE_METRICS_SAMPLES = 20000  # Increase to 20k
```

### Change Slow Operation Threshold

Default is 5 seconds. To adjust:

```python
# In main.py, in record_storage_operation function
if duration_ms > 3000:  # Change to 3 seconds
    logger.warning(...)
```

### Customize Metrics Window

Default metrics window is 5 minutes. To change:

```python
# In get_storage_metrics function
recent_window = 600  # Change to 10 minutes
```

## Comparison with AWS S3 and Azure Blob

### Similar to AWS S3 Metrics:
- Request rate monitoring
- Error rate tracking  
- Latency percentiles (P50, P95, P99)
- Operation-level breakdown
- Data transfer metrics

### Similar to Azure Blob Storage Analytics:
- Operation success/failure rates
- Per-operation metrics
- Transaction analytics
- Bandwidth utilization
- Access pattern analysis

### Additional SpaceWatch Features:
- AI-powered query interface
- Integrated access log search
- Real-time operation logs
- Automatic slow operation detection
- Operation correlation with request IDs
