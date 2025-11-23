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

const getSchema = async (env: Env): Promise<ValueResult<ParquetSchema>> => {
  const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
  if (!schema) {
    return {
      success: false,
      errors: ['No schema'],
    };
  }

  const schemaText = await schema?.text();
  const schemaJSON = SafeJSONParse(schemaText);
  if (!schemaJSON.success) {
    return {
      success: false,
      errors: ['Schema is not valid JSON'],
    };
  }

  const parseResult = ParquetSchema.safeParse(schemaJSON.value);
  if (parseResult.success) {
    return {
      success: true,
      value: parseResult.data,
    };
  }

  return {
    success: false,
    errors: [JSON.stringify(parseResult.error)],
  };
};

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

          const addedFiles: FileAction[] = [];
          const removedFiles: FileAction[] = [];
          let totalRows = 0;

          // Process each partition independently
          for (const [partitionKey, partitionFiles] of partitionMap.entries()) {
            // Read files for this partition
            const errors: string[] = [];
            const fileTexts: string[] = [];

            for (const key of partitionFiles) {
              const obj = await env.LAKESIDE_BUCKET.get(key);
              const objText = await obj?.text();
              if (!objText) {
                errors.push(`No text for ${key}`);
              } else {
                fileTexts.push(objText);
              }
            }

            if (errors.length > 0) {
              await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
              return ErrorsToResponse(errors);
            }

            // Parse JSON strings
            const fileJSONs = fileTexts.map(SafeJSONParse);
            const fileJSONErrors = fileJSONs.filter((r) => !r.success);
            if (fileJSONErrors.length > 0) {
              await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
              return ErrorsToResponse(
                fileJSONErrors.map((e) => !e.success ? e.errors.map((e) => JSON.stringify(e)).join(', ') : "").flat()
              );
            }

            const fileJSONValues = fileJSONs.map((r) =>
              r.success ? JSON.stringify(r.value) : undefined).filter((v) => v !== undefined) as string[];

            const rowCount = fileJSONValues.length;
            totalRows += rowCount;

            // Generate parquet file with Hive-style partitioning
            const parquetFile = generateParquet(JSON.stringify(schemaResult.value), fileJSONValues);
            const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
            const parquetKey = `parquet/${partitionKey}/part-${timestamp}.parquet`;

            // Write parquet to staging area first
            const stagingKey = `_staging/${partitionKey}/part-${timestamp}.parquet`;
            await env.LAKESIDE_BUCKET.put(stagingKey, parquetFile);

            // Track file actions for transaction log
            addedFiles.push({
              path: parquetKey,
              size: parquetFile.byteLength,
              rowCount: rowCount,
              partition: partitionKey,
            });

            for (const fileKey of partitionFiles) {
              removedFiles.push({ path: fileKey });
            }
          }

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

          // Move files from staging to final location
          for (const file of addedFiles) {
            const stagingKey = `_staging/${file.partition}/part-${file.path.split('/').pop()}`;
            const content = await env.LAKESIDE_BUCKET.get(stagingKey);
            if (content) {
              const buffer = await content.arrayBuffer();
              await env.LAKESIDE_BUCKET.put(file.path, buffer);
              await env.LAKESIDE_BUCKET.delete(stagingKey);
            }
          }

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
