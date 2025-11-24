# DuckDB Integration Guide

Now that Lakeside uses Hive-style partitioning and transaction logs, you can query the data lake with DuckDB!

## Prerequisites

1. **Install DuckDB**
   ```bash
   # macOS
   brew install duckdb

   # Or download from https://duckdb.org/docs/installation/
   ```

2. **Configure R2 S3 Access**

   Get your R2 credentials from Cloudflare dashboard:
   - Account ID: Found in Cloudflare dashboard
   - Access Key ID: Create in R2 settings
   - Secret Access Key: Shown when creating access key

## Local Querying (Download Parquet Files)

If you've downloaded the parquet files locally:

```sql
-- Start DuckDB
duckdb

-- Query a single partition
SELECT * FROM read_parquet('parquet/order_ts_hour=2025-11-23T19/*.parquet');

-- Query all partitions with glob pattern
SELECT * FROM read_parquet('parquet/**/*.parquet');

-- Query with partition pruning (much faster!)
SELECT *
FROM read_parquet('parquet/**/*.parquet', hive_partitioning=1)
WHERE order_ts_hour = '2025-11-23T19'
LIMIT 10;

-- Aggregate across all partitions
SELECT
    order_ts_hour,
    COUNT(*) as row_count
FROM read_parquet('parquet/**/*.parquet', hive_partitioning=1)
GROUP BY order_ts_hour
ORDER BY order_ts_hour DESC;
```

## Remote Querying (Direct from R2)

Query directly from Cloudflare R2 using S3 API:

```sql
-- Install and load httpfs extension
INSTALL httpfs;
LOAD httpfs;

-- Configure R2 endpoint
SET s3_endpoint='<account-id>.r2.cloudflarestorage.com';
SET s3_access_key_id='<your-access-key>';
SET s3_secret_access_key='<your-secret-key>';
SET s3_url_style='path';

-- Query from R2
SELECT *
FROM read_parquet('s3://lakeside/parquet/**/*.parquet', hive_partitioning=1)
WHERE order_ts_hour >= '2025-11-23T19'
LIMIT 100;

-- Create a view for easy access
CREATE VIEW lakeside_data AS
SELECT *
FROM read_parquet('s3://lakeside/parquet/**/*.parquet', hive_partitioning=1);

-- Now query the view
SELECT order_ts_hour, COUNT(*)
FROM lakeside_data
GROUP BY order_ts_hour;
```

## Understanding the Transaction Log

The transaction log ensures ACID properties. You can query it to understand the data lake state:

```bash
# View all transactions
curl http://compactor-url/transactions

# Example output:
# [
#   {
#     "version": 0,
#     "timestamp": "2025-11-23T19:30:45Z",
#     "operation": "compact",
#     "add": [
#       {
#         "path": "parquet/order_ts_hour=2025-11-23T19/part-12345.parquet",
#         "size": 245000,
#         "rowCount": 1500,
#         "partition": "order_ts_hour=2025-11-23T19"
#       }
#     ],
#     "remove": [
#       {"path": "data/order_ts_hour=2025-11-23T19/abc123.json"}
#     ]
#   }
# ]
```

## Partition Structure

Lakeside now uses Hive-style partitioning:

```
lakeside/
├── data/                          # Staging (JSON)
│   └── order_ts_hour=2025-11-23T19/
│       ├── abc123.json
│       └── def456.json
├── parquet/                       # Compacted (Parquet)
│   ├── order_ts_hour=2025-11-23T19/
│   │   └── part-2025-11-23T19-30-45.parquet
│   └── order_ts_hour=2025-11-23T20/
│       └── part-2025-11-23T20-15-30.parquet
└── _lakeside_log/                 # Transaction log
    ├── 00000000.json
    └── 00000001.json
```

## Querying Benefits

### 1. Partition Pruning
DuckDB only reads relevant partitions:

```sql
-- Only scans order_ts_hour=2025-11-23T19 partition
SELECT *
FROM read_parquet('parquet/**/*.parquet', hive_partitioning=1)
WHERE order_ts_hour = '2025-11-23T19';
```

### 2. Columnar Efficiency
Parquet stores data column-wise, so you only read what you need:

```sql
-- Only reads 'name' column, skips others
SELECT name
FROM read_parquet('parquet/**/*.parquet');
```

### 3. Parallel Scanning
DuckDB automatically parallelizes reads across multiple parquet files.

## Advanced Queries

### Time-Range Queries
```sql
SELECT *
FROM read_parquet('s3://lakeside/parquet/**/*.parquet', hive_partitioning=1)
WHERE order_ts_hour BETWEEN '2025-11-23T10' AND '2025-11-23T20'
ORDER BY order_ts_hour;
```

### Aggregations with Partitions
```sql
SELECT
    order_ts_hour,
    COUNT(*) as orders,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM read_parquet('s3://lakeside/parquet/**/*.parquet', hive_partitioning=1)
GROUP BY order_ts_hour
ORDER BY order_ts_hour DESC;
```

### Export Results
```sql
-- Export to CSV
COPY (
    SELECT *
    FROM read_parquet('parquet/**/*.parquet', hive_partitioning=1)
    WHERE order_ts_hour = '2025-11-23T19'
) TO 'results.csv' (HEADER, DELIMITER ',');

-- Export to new Parquet file
COPY (
    SELECT * FROM lakeside_data WHERE order_ts_hour >= '2025-11-23T00'
) TO 'filtered.parquet' (FORMAT PARQUET);
```

## Reconciliation & Monitoring

### Check for Orphaned Files
```bash
# Find files that should have been deleted but still exist
curl http://compactor-url/reconcile

# Cleanup orphaned files
curl -X DELETE http://compactor-url/cleanup
```

### Monitor Compaction Status
```bash
# Check if compaction is running
curl http://compactor-url/

# Output:
# {
#   "isCompacting": true,
#   "currentBatch": ["data/order_ts_hour=2025-11-23T19/file1.json"],
#   "startedAt": 1700000000000
# }
```

## Performance Tips

1. **Use Partition Filters**: Always filter on `order_ts_hour` when possible
2. **Limit Columns**: Only SELECT the columns you need
3. **Use Views**: Create views for common query patterns
4. **Batch Queries**: Process multiple partitions in one query rather than many small queries
5. **Local Caching**: Download frequently-accessed partitions locally for faster queries

## Example: Real-Time Analytics

```sql
-- Dashboard query: Last 24 hours of data
CREATE VIEW recent_data AS
SELECT *
FROM read_parquet('s3://lakeside/parquet/**/*.parquet', hive_partitioning=1)
WHERE order_ts_hour >= strftime(current_timestamp - INTERVAL '24 hours', '%Y-%m-%dT%H');

-- Get hourly metrics
SELECT
    order_ts_hour,
    COUNT(*) as count,
    MIN(created_at) as first_event,
    MAX(created_at) as last_event
FROM recent_data
GROUP BY order_ts_hour
ORDER BY order_ts_hour DESC;
```

## Troubleshooting

### "File not found" Error
- Check that R2 credentials are correct
- Verify bucket name is "lakeside"
- Ensure parquet files exist: `curl http://compactor-url/transactions`

### Slow Queries
- Ensure you're using `hive_partitioning=1`
- Check if partition filters are applied
- Verify DuckDB is using partition pruning (EXPLAIN query)

### Empty Results
- Check transaction log to see what files were created
- Verify partition naming matches your WHERE clause
- List R2 bucket contents to confirm files exist
