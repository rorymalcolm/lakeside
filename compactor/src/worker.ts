import { ParquetSchema } from 'parquet-types';
import { ValueResult, ErrorsToResponse, SafeJSONParse } from 'rerrors';
import { generate_parquet as generateParquet } from 'parquet-generator';

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
          // Read ONLY the files we listed (prevents race with new writes)
          const errors: string[] = [];
          const fileTexts: string[] = [];

          for (const key of fileKeys) {
            const obj = await env.LAKESIDE_BUCKET.get(key);
            const objText = await obj?.text();
            if (!objText) {
              errors.push(`No text for ${key}`);
            } else {
              fileTexts.push(objText);
            }
          }

          if (errors.length > 0) {
            // Release lock before returning error
            await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
            return ErrorsToResponse(errors);
          }

          // Parse JSON strings
          const fileJSONs = fileTexts.map(SafeJSONParse);
          const fileJSONErrors = fileJSONs.filter((r) => !r.success);
          if (fileJSONErrors.length > 0) {
            // Release lock before returning error
            await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
            return ErrorsToResponse(
              fileJSONErrors.map((e) => !e.success ? e.errors.map((e) => JSON.stringify(e)).join(', ') : "").flat()
            );
          }

          const fileJSONValues = fileJSONs.map((r) =>
            r.success ? JSON.stringify(r.value) : undefined).filter((v) => v !== undefined) as string[];

          // Generate parquet file with timestamp
          const parquetFile = generateParquet(JSON.stringify(schemaResult.value), fileJSONValues);
          const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
          const parquetKey = `parquet/data-${timestamp}.parquet`;

          const putRes = await env.LAKESIDE_BUCKET.put(parquetKey, parquetFile);
          if (!putRes) {
            // Release lock before returning error
            await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
            return new Response('Failed to write parquet file', { status: 500 });
          }

          // CRITICAL: Only delete the EXACT files we compacted (from snapshot)
          // Files written AFTER our snapshot will NOT be deleted
          const deletePromises = fileKeys.map(key => env.LAKESIDE_BUCKET.delete(key));
          await Promise.all(deletePromises);

          // Release lock after successful compaction
          await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));

          return new Response(`OK - Created ${parquetKey} and deleted ${fileKeys.length} JSON files`);
        } catch (e) {
          // Ensure lock is released even if compaction fails
          await coordinator.fetch(new Request('http://internal/release', { method: 'POST' }));
          throw e;
        }
      }

      // GET endpoint to check compaction status
      if (request.method === 'GET') {
        const coordinatorId = env.COMPACTION_COORDINATOR.idFromName('global-compaction-lock');
        const coordinator = env.COMPACTION_COORDINATOR.get(coordinatorId);
        const statusRes = await coordinator.fetch(new Request('http://internal/status'));
        return statusRes;
      }
      return new Response('', { status: 405 });
    } catch (e) {
      console.error('Error processing request:', e);
      return new Response(`Failed to process request: ${e instanceof Error ? e.message : String(e)}`, { status: 500 });
    }
  },
};
