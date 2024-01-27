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
  const files = await env.LAKESIDE_BUCKET.list({ prefix: 'json/' });
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
  const fileJSONResults = await Promise.all(fileJSONs);
  const fileJSONErrors = fileJSONResults.filter((r) => !r.success);
  if (fileJSONErrors.length > 0) {
    return {
      success: false,
      errors: fileJSONErrors.map((e) => !e.success ?
        e.errors.map((e) => JSON.stringify(e)).join(', ') : ""
      ).flat(),
    };
  }

  const fileJSONValues = fileJSONResults.map((r) =>
    r.success ? r.value : undefined).filter((v) => v !== undefined) as string[];
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
        const parquetFile = generateParquet(JSON.stringify(schemaResult.value), filesResult.value);
        const putRes = await env.LAKESIDE_BUCKET.put('parquet/parquet.parquet', parquetFile);
        if (!putRes) {
          return new Response('', { status: 500 });
        }
        return new Response('OK');
      }
      return new Response('', { status: 405 });
    } catch (e) {
      return new Response('failed to process request', { status: 500 });
    }
  },
};
