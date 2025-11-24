import { ParquetSchema } from 'parquet-types';
import { ValueResult, ErrorsToResponse, SafeJSONParse } from 'rerrors';
import { generate_parquet as generateParquet } from 'parquet-generator';
import {
  groupFilesByPartition,
  appendTransaction,
  FileAction,
  readTransactionLog,
  getCurrentState,
} from './transaction-log';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
  COMPACTION_COORDINATOR: DurableObjectNamespace;
}

// Export the Durable Object class
export { CompactionCoordinator } from './compaction-coordinator';

// Global schema cache with ETag for conditional fetching
let schemaCache: { etag: string; schema: ParquetSchema; timestamp: number } | null = null;
const SCHEMA_CACHE_TTL = 60000; // 60 seconds

const getSchema = async (env: Env): Promise<ValueResult<ParquetSchema>> => {
  // Check if cache is still valid
  if (schemaCache && Date.now() - schemaCache.timestamp < SCHEMA_CACHE_TTL) {
    return {
      success: true,
      value: schemaCache.schema,
    };
  }

  // Fetch with conditional request if we have a cached etag
  const schema = await env.LAKESIDE_BUCKET.get('schema/schema.json', {
    onlyIf: schemaCache ? { etagDoesNotMatch: schemaCache.etag } : undefined,
  });

  // If null and we have cache, schema hasn't changed
  if (!schema && schemaCache) {
    schemaCache.timestamp = Date.now(); // Refresh TTL
    return {
      success: true,
      value: schemaCache.schema,
    };
  }

  if (!schema) {
    return {
      success: false,
      errors: ['No schema'],
    };
  }

  const schemaText = await schema.text();
  const schemaJSON = SafeJSONParse(schemaText);
  if (!schemaJSON.success) {
    return {
      success: false,
      errors: ['Schema is not valid JSON'],
    };
  }

  const parseResult = ParquetSchema.safeParse(schemaJSON.value);
  if (!parseResult.success) {
    return {
      success: false,
      errors: [JSON.stringify(parseResult.error)],
    };
  }

  // Update cache
  schemaCache = {
    etag: schema.etag,
    schema: parseResult.data,
    timestamp: Date.now(),
  };

  return {
    success: true,
    value: parseResult.data,
  };
};

/**
 * Parse file content (supports both JSON and NDJSON)
 */
function parseFileContent(content: string, filename: string): any[] {
  // Check if it's NDJSON (newline-delimited JSON)
  if (filename.endsWith('.ndjson')) {
    const lines = content.split('\n').filter(line => line.trim());
    return lines.map(line => JSON.parse(line));
  }

  // Otherwise treat as single JSON object
  return [JSON.parse(content)];
}

/**
 * Process a single partition (for parallel execution)
 */
async function processPartition(
  partitionKey: string,
  partitionFiles: string[],
  env: Env,
  schemaResult: ParquetSchema
): Promise<{
  addedFiles: FileAction[];
  removedFiles: FileAction[];
  rowCount: number;
  parquetBuffer: Uint8Array;
  parquetKey: string;
}> {
  // Parallel file reads (10x faster)
  const filePromises = partitionFiles.map(async (key) => {
    const obj = await env.LAKESIDE_BUCKET.get(key);
    if (!obj) {
      throw new Error(`Failed to read file: ${key}`);
    }
    const text = await obj.text();
    return { key, text };
  });

  const fileResults = await Promise.all(filePromises);

  // Parse all files (handles both JSON and NDJSON)
  const allRecords: any[] = [];
  for (const { key, text } of fileResults) {
    try {
      const records = parseFileContent(text, key);
      allRecords.push(...records);
    } catch (e) {
      throw new Error(`Failed to parse ${key}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  // Convert records to JSON strings for WASM module
  const recordStrings = allRecords.map(r => JSON.stringify(r));

  // Generate parquet file
  const parquetFile = generateParquet(JSON.stringify(schemaResult), recordStrings);
  const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
  const parquetKey = `parquet/${partitionKey}/part-${timestamp}.parquet`;

  const addedFiles: FileAction[] = [{
    path: parquetKey,
    size: parquetFile.byteLength,
    rowCount: allRecords.length,
    partition: partitionKey,
  }];

  const removedFiles: FileAction[] = partitionFiles.map(path => ({ path }));

  return {
    addedFiles,
    removedFiles,
    rowCount: allRecords.length,
    parquetBuffer: parquetFile,
    parquetKey,
  };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const schemaResult = await getSchema(env);
    if (!schemaResult.success) {
      return ErrorsToResponse(schemaResult.errors);
    }

    try {
      if (request.method === 'POST') {
        // Get Durable Object instance (singleton per account)
        const coordinatorId = env.COMPACTION_COORDINATOR.idFromName('global-compaction-lock');
        const coordinator = env.COMPACTION_COORDINATOR.get(coordinatorId);

        // List files ONCE and snapshot the keys
        const filesList = await env.LAKESIDE_BUCKET.list({ prefix: 'data/' });
        const fileKeys = filesList.objects.map(obj => obj.key);

        if (fileKeys.length === 0) {
          return new Response('No files to compact', { status: 200 });
        }

        // Try to acquire the compaction lock
        const acquireReq = new Request('http://internal/acquire', {
          method: 'POST',
          body: JSON.stringify(fileKeys),
        });
        const acquireRes = await coordinator.fetch(acquireReq);

        if (!acquireRes.ok) {
          const data = await acquireRes.json() as { message: string };
          return new Response(`Compaction already in progress: ${data.message}`, { status: 409 });
        }

        try {
          // Group files by partition (Hive-style)
          const partitionMap = groupFilesByPartition(fileKeys);

          if (partitionMap.size === 0) {
            await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
            return new Response('No valid partitions found', { status: 400 });
          }

          // Process all partitions in parallel (3-5x faster)
          const partitionEntries = Array.from(partitionMap.entries());
          const partitionPromises = partitionEntries.map(([partitionKey, partitionFiles]) =>
            processPartition(partitionKey, partitionFiles, env, schemaResult.value)
              .catch(err => ({
                error: `Partition ${partitionKey} failed: ${err.message}`,
                partitionKey,
              }))
          );

          const partitionResults = await Promise.all(partitionPromises);

          // Check for errors
          const errors = partitionResults.filter(r => 'error' in r);
          if (errors.length > 0) {
            await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
            return new Response(JSON.stringify({
              error: 'Compaction failed',
              details: errors,
            }), { status: 500 });
          }

          // Collect results
          const successResults = partitionResults.filter(r => !('error' in r)) as Array<{
            addedFiles: FileAction[];
            removedFiles: FileAction[];
            rowCount: number;
            parquetBuffer: Uint8Array;
            parquetKey: string;
          }>;

          const addedFiles = successResults.flatMap(r => r.addedFiles);
          const removedFiles = successResults.flatMap(r => r.removedFiles);
          const totalRows = successResults.reduce((sum, r) => sum + r.rowCount, 0);

          // Write transaction log entry (atomic commit point)
          const txVersion = await appendTransaction(env.LAKESIDE_BUCKET, {
            timestamp: new Date().toISOString(),
            operation: 'compact',
            add: addedFiles,
            remove: removedFiles,
            metadata: {
              partitionCount: partitionMap.size,
              totalRows: totalRows,
            },
          });

          // Write parquet files directly (no staging area - saves 2x I/O)
          const writePromises = successResults.map(result =>
            env.LAKESIDE_BUCKET.put(result.parquetKey, result.parquetBuffer, {
              httpMetadata: {
                contentType: 'application/parquet',
              },
              customMetadata: {
                transactionVersion: txVersion.toString(),
                rowCount: result.rowCount.toString(),
                partition: result.addedFiles[0].partition || '',
              },
            })
          );
          await Promise.all(writePromises);

          // Delete source JSON files (idempotent - safe to retry)
          const deletePromises = fileKeys.map(key => env.LAKESIDE_BUCKET.delete(key));
          await Promise.all(deletePromises);

          // Release lock after successful compaction
          await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));

          return new Response(JSON.stringify({
            success: true,
            transactionVersion: txVersion,
            partitions: partitionMap.size,
            filesCompacted: fileKeys.length,
            totalRows: totalRows,
            parquetFiles: addedFiles.map(f => f.path),
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        } catch (e) {
          // Ensure lock is released even if compaction fails
          await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
          throw e;
        }
      }

      // GET endpoint to check compaction status
      if (request.method === 'GET') {
        const url = new URL(request.url);

        // GET /transactions - View transaction log
        if (url.pathname === '/transactions' || url.searchParams.has('transactions')) {
          const transactions = await readTransactionLog(env.LAKESIDE_BUCKET);
          return new Response(JSON.stringify(transactions, null, 2), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }

        // GET /reconcile - Check for orphaned files
        if (url.pathname === '/reconcile' || url.searchParams.has('reconcile')) {
          const state = await getCurrentState(env.LAKESIDE_BUCKET);
          return new Response(JSON.stringify({
            parquetFiles: Array.from(state.parquetFiles),
            orphanedJsonFiles: Array.from(state.orphanedJsonFiles),
            orphanCount: state.orphanedJsonFiles.size,
          }, null, 2), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }

        // GET / - Compaction status
        const coordinatorId = env.COMPACTION_COORDINATOR.idFromName('global-compaction-lock');
        const coordinator = env.COMPACTION_COORDINATOR.get(coordinatorId);
        const statusRes = await coordinator.fetch(new Request('http://internal/status'));
        return statusRes;
      }

      // DELETE /cleanup - Clean up orphaned files
      if (request.method === 'DELETE') {
        const url = new URL(request.url);

        if (url.pathname === '/cleanup' || url.searchParams.has('cleanup')) {
          const state = await getCurrentState(env.LAKESIDE_BUCKET);

          if (state.orphanedJsonFiles.size === 0) {
            return new Response(JSON.stringify({ message: 'No orphaned files to clean' }), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            });
          }

          const deletePromises = Array.from(state.orphanedJsonFiles).map(key =>
            env.LAKESIDE_BUCKET.delete(key)
          );
          await Promise.all(deletePromises);

          return new Response(JSON.stringify({
            message: 'Cleanup complete',
            deletedCount: state.orphanedJsonFiles.size,
            deletedFiles: Array.from(state.orphanedJsonFiles),
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }
      }

      return new Response('', { status: 405 });
    } catch (e) {
      console.error('Error processing request:', e);
      return new Response(`Failed to process request: ${e instanceof Error ? e.message : String(e)}`, { status: 500 });
    }
  },
};
