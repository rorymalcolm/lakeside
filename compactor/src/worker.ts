import { ParquetSchema } from 'parquet-types';
import { ValueResult, ErrorsToResponse, SafeJSONParse } from 'rerrors';
import { generate_parquet as generateParquet } from 'parquet-generator';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

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

const getFiles = async (env: Env): Promise<ValueResult<string[]>> => {
  const files = await env.LAKESIDE_BUCKET.list({ prefix: 'data/' });
  if (!files) {
    return {
      success: false,
      errors: ['No files'],
    };
  }

  const errors: string[] = [];
  const fileTexts = await Promise.all(files.objects.map(async (f) => {
    const obj = await env.LAKESIDE_BUCKET.get(f.key)
    const objText = await obj?.text();
    if (!objText) {
      errors.push(`No text for ${f.key}`);
    }
    return objText;
  }));

  if (errors.length > 0) {
    return {
      success: false,
      errors,
    };
  }

  const fileJSONs = (fileTexts as string[]).map(SafeJSONParse);
  const fileJSONErrors = fileJSONs.filter((r) => !r.success);
  if (fileJSONErrors.length > 0) {
    return {
      success: false,
      errors: fileJSONErrors.map((e) => !e.success ?
        e.errors.map((e) => JSON.stringify(e)).join(', ') : ""
      ).flat(),
    };
  }

  const fileJSONValues = fileJSONs.map((r) =>
    r.success ? JSON.stringify(r.value) : undefined).filter((v) => v !== undefined) as string[];
  return {
    success: true,
    value: fileJSONValues,
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const schemaResult = await getSchema(env);
    if (!schemaResult.success) {
      return ErrorsToResponse(schemaResult.errors);
    }

    try {
      if (request.method === 'POST') {
        const filesResult = await getFiles(env);
        if (!filesResult.success) {
          return ErrorsToResponse(filesResult.errors);
        }

        // Get list of files before compaction for cleanup
        const filesList = await env.LAKESIDE_BUCKET.list({ prefix: 'data/' });
        const fileKeys = filesList.objects.map(obj => obj.key);

        // Generate parquet file with timestamp
        const parquetFile = generateParquet(JSON.stringify(schemaResult.value), filesResult.value);
        const timestamp = new Date().toISOString().replace(/:/g, '-').replace(/\..+/, '');
        const parquetKey = `parquet/data-${timestamp}.parquet`;

        const putRes = await env.LAKESIDE_BUCKET.put(parquetKey, parquetFile);
        if (!putRes) {
          return new Response('Failed to write parquet file', { status: 500 });
        }

        // Cleanup: delete the original JSON files after successful compaction
        const deletePromises = fileKeys.map(key => env.LAKESIDE_BUCKET.delete(key));
        await Promise.all(deletePromises);

        return new Response(`OK - Created ${parquetKey} and deleted ${fileKeys.length} JSON files`);
      }
      return new Response('', { status: 405 });
    } catch (e) {
      console.error('Error processing request:', e);
      return new Response(`Failed to process request: ${e instanceof Error ? e.message : String(e)}`, { status: 500 });
    }
  },
};
