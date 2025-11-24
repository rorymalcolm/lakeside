# Lakeside Production Roadmap

## Current State: ⚠️ NOT Production Ready

The system has core functionality but **cannot be queried** and has **no guarantees against data loss**.

---

## Blocking Issues for Production MVP

### 🔴 CRITICAL - Data Loss Risks

#### 1. No Atomic Cleanup
**Problem:** Parquet write succeeds but deletes fail partway through
```typescript
// Current: No transaction guarantees
await env.LAKESIDE_BUCKET.put(parquetKey, parquetFile);  // ✅ Success
await Promise.all(fileKeys.map(key => delete(key)));     // ❌ 3/5 fail
// Result: Files orphaned, data duplicated
```

**Solution:** Two-phase commit
```typescript
// Phase 1: Write parquet + manifest
await put(parquetKey, parquetFile);
await put(`manifests/${timestamp}.json`, { files: fileKeys });

// Phase 2: Mark as committed (atomic)
await put(`_committed/${timestamp}`, '');

// Phase 3: Async cleanup (idempotent)
await deleteFiles(fileKeys);  // Can retry safely
```

#### 2. No Transaction Log
**Problem:** No way to know what data exists or recover from failures

**Solution:** Implement Delta Lake-style transaction log
```
_delta_log/
  00000.json  ← add parquet/data-1.parquet
  00001.json  ← add parquet/data-2.parquet, remove json files
  00002.json  ← schema change
```

#### 3. Schema Evolution Breaks Compaction
**Problem:** Schema changes mid-compaction cause data corruption

**Solution:** Version schemas
```typescript
interface ParquetSchemaVersioned {
  version: number;
  schema: ParquetSchema;
  timestamp: string;
}

// Each parquet file stores its schema version
// Compaction only processes files with same schema version
```

---

### 🔴 CRITICAL - DuckDB Cannot Query

#### 4. No Partition Preservation
**Problem:** Time partitions lost during compaction
```
Input:  data/order_ts_hour=2025-11-23T19/file1.json
Output: parquet/data-2025-11-23T19-30-45.parquet  ❌
```

**Solution:** Preserve Hive partitioning
```
parquet/
  order_ts_hour=2025-11-23T19/
    part-00000.parquet
  order_ts_hour=2025-11-23T20/
    part-00000.parquet
```

**DuckDB Query:**
```sql
SELECT * FROM read_parquet('s3://lakeside/parquet/**/*.parquet')
WHERE order_ts_hour = '2025-11-23T19';
-- Uses partition pruning! ⚡
```

#### 5. No Data Catalog
**Problem:** DuckDB doesn't know what files exist

**Solution:** Manifest file per compaction
```json
{
  "version": 1,
  "timestamp": "2025-11-23T19:30:45Z",
  "partitions": [
    {
      "partition": "order_ts_hour=2025-11-23T19",
      "files": ["part-00000.parquet"],
      "row_count": 1500,
      "size_bytes": 245000
    }
  ]
}
```

#### 6. Missing R2 S3 API Configuration
**DuckDB Setup Needed:**
```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='<account-id>.r2.cloudflarestorage.com';
SET s3_access_key_id='<key>';
SET s3_secret_access_key='<secret>';

-- Then can query:
SELECT * FROM read_parquet('s3://lakeside/parquet/**/*.parquet');
```

---

### 🟡 HIGH Priority - Operability

#### 7. No Automatic Compaction
**Current:** Manual POST required
**Need:** Cloudflare Cron Trigger
```toml
# wrangler.toml
[triggers]
crons = ["*/15 * * * *"]  # Every 15 minutes
```

#### 8. No Monitoring
**Need:**
- Compaction success/failure metrics
- Row counts, file sizes
- Lock contention alerts
- Orphaned file detection

#### 9. No Tests
```bash
├── gateway/
│   ├── src/
│   └── tests/          ❌ Missing
├── compactor/
│   ├── src/
│   └── tests/          ❌ Missing
└── parquet-generator/
    ├── src/
    └── tests/          ❌ Missing
```

---

## Minimal Production MVP (Week 1-2 scope)

### Phase 1: Make it Queryable (2-3 days)
- [ ] Preserve Hive partitioning in parquet output
- [ ] Add manifest.json per compaction
- [ ] Document DuckDB connection setup
- [ ] Basic integration test with DuckDB

### Phase 2: Prevent Data Loss (2-3 days)
- [ ] Two-phase commit for cleanup
- [ ] Manifest-based reconciliation
- [ ] Orphaned file detector (cron job)
- [ ] Schema versioning

### Phase 3: Automatic Operation (1-2 days)
- [ ] Cron-triggered compaction
- [ ] Basic metrics/logging
- [ ] Health check endpoints
- [ ] Simple monitoring dashboard

### Phase 4: Production Hardening (2-3 days)
- [ ] Unit + integration tests (70% coverage)
- [ ] Error recovery procedures
- [ ] Runbook documentation
- [ ] Load testing

---

## Future Enhancements (Post-MVP)

### Query Layer
- Integrate with DuckDB WASM for in-browser queries
- GraphQL/REST API over parquet data
- Real-time queries (query JSON before compaction)

### Performance
- Parquet compression (SNAPPY/ZSTD)
- Column statistics for predicate pushdown
- Adaptive row group sizing
- Concurrent compaction (multiple partitions)

### Features
- Multi-table support
- JOIN support across tables
- Time-travel queries (transaction log)
- Schema evolution (add/drop columns)
- Data retention policies
- Incremental backups

### Enterprise
- Multi-tenancy
- RBAC (row/column level security)
- Audit logging
- Data encryption at rest
- Compliance (GDPR, SOC2)

---

## Recommended Architecture Changes

### Add Transaction Log (Delta Lake style)
```
lakeside/
  _lakeside/
    00000.json       ← Transaction log
    00001.json
  data/              ← JSON staging (unchanged)
  parquet/           ← Hive-partitioned parquet
    order_ts_hour=2025-11-23T19/
      part-00000.parquet
    order_ts_hour=2025-11-23T20/
      part-00001.parquet
```

### Compaction Flow with Guarantees
```typescript
async function compact() {
  // 1. Acquire lock
  const lock = await coordinator.acquire();

  // 2. Snapshot files (grouped by partition)
  const partitions = await snapshotPartitions();

  // 3. For each partition, create parquet
  for (const partition of partitions) {
    const parquetFile = await generateParquet(partition.files);

    // 4. Write to staging
    await put(`_staging/${partition.key}/part.parquet`, parquetFile);

    // 5. Write manifest
    await put(`_staging/${partition.key}/manifest.json`, {
      sourceFiles: partition.fileKeys,
      rowCount: partition.rowCount,
    });
  }

  // 6. Atomic commit (write transaction log entry)
  await appendToTransactionLog({
    version: nextVersion,
    timestamp: now,
    operation: 'compact',
    add: partitions.map(p => `parquet/${p.key}/part.parquet`),
    remove: partitions.flatMap(p => p.fileKeys),
  });

  // 7. Move staging to final location
  for (const partition of partitions) {
    await rename(`_staging/${partition.key}`, `parquet/${partition.key}`);
  }

  // 8. Async cleanup (can retry if fails)
  await scheduleCleanup(partitions.flatMap(p => p.fileKeys));

  // 9. Release lock
  await lock.release();
}
```

---

## DuckDB Integration Example

```typescript
// Query service worker
export default {
  async fetch(request: Request, env: Env) {
    const query = await request.json();

    // Read transaction log to find relevant files
    const manifest = await getLatestManifest(env);

    // Generate S3 presigned URLs for DuckDB
    const urls = manifest.files.map(f =>
      generatePresignedUrl(env.LAKESIDE_BUCKET, f)
    );

    // Return query plan
    return new Response(JSON.stringify({
      sql: `SELECT * FROM read_parquet([${urls.join(',')}])`,
      partitions: manifest.partitions,
    }));
  }
}
```

---

## Success Metrics

**Production MVP is ready when:**
- ✅ DuckDB can successfully query the data
- ✅ Zero data loss in 1000 compaction cycles
- ✅ Automatic compaction runs without intervention
- ✅ Failed compactions self-recover within 1 hour
- ✅ 70%+ test coverage
- ✅ Monitoring shows <1% error rate
- ✅ Documentation allows new dev to deploy in <1 hour

**Current Status:** 0/7 ✅ (Not production ready)
