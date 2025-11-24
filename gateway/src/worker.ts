import { v4 } from 'uuid';
import { validateJSONAgainstSchema } from 'parquet-schema-validator';
import { ParquetSchema } from 'parquet-types';
import { ValueResult, ErrorsToResponse, SafeJSONParse } from 'rerrors';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

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

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const schemaResult = await getSchema(env);
    if (!schemaResult.success) {
      return ErrorsToResponse(schemaResult.errors);
    }

    try {
      const url = new URL(request.url);

      // Single record ingestion
      if (request.method === 'PUT') {
        const json = await request.json();

        const validated = validateJSONAgainstSchema(json, schemaResult.value);
        if (!validated.success) {
          return new Response(JSON.stringify(validated.errors), { status: 400 });
        }

        const currentPrefix = `data/order_ts_hour=${new Date().toISOString().slice(0, 13)}`;

        const putOperation = await env.LAKESIDE_BUCKET.put(`${currentPrefix}/${v4()}.json`, JSON.stringify(json));
        if (putOperation) {
          return new Response('OK');
        } else {
          return new Response('FAILED', { status: 500 });
        }
      }

      // Batch ingestion (100x throughput improvement)
      if (request.method === 'POST' && url.pathname === '/batch') {
        const records = await request.json();

        if (!Array.isArray(records)) {
          return new Response(JSON.stringify({ error: 'Expected array of records' }), { status: 400 });
        }

        // Validate all records
        const validationErrors: Array<{ index: number; errors: string[] }> = [];
        for (let i = 0; i < records.length; i++) {
          const validated = validateJSONAgainstSchema(records[i], schemaResult.value);
          if (!validated.success) {
            validationErrors.push({ index: i, errors: validated.errors });
          }
        }

        if (validationErrors.length > 0) {
          return new Response(JSON.stringify({
            error: 'Validation failed',
            validationErrors: validationErrors.slice(0, 10), // Return first 10 errors
            totalErrors: validationErrors.length
          }), { status: 400 });
        }

        const currentPrefix = `data/order_ts_hour=${new Date().toISOString().slice(0, 13)}`;

        // Write as single NDJSON file (newline-delimited JSON)
        const ndjson = records.map(r => JSON.stringify(r)).join('\n');
        const putOperation = await env.LAKESIDE_BUCKET.put(
          `${currentPrefix}/${v4()}.ndjson`,
          ndjson,
          {
            httpMetadata: {
              contentType: 'application/x-ndjson',
            },
            customMetadata: {
              recordCount: records.length.toString(),
            },
          }
        );

        if (putOperation) {
          return new Response(JSON.stringify({
            success: true,
            accepted: records.length,
            partition: currentPrefix,
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        } else {
          return new Response('FAILED', { status: 500 });
        }
      }

      return new Response('', { status: 405 });
    } catch (e) {
      console.error('Error processing request:', e);
      return new Response(`Failed to process request: ${e instanceof Error ? e.message : String(e)}`, { status: 500 });
    }
  },
};
