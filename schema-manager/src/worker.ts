import { ParquetSchema, ParquetSchemaUpdateRequest } from 'parquet-types';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

async function processGETSchema(env: Env) {
  const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
  if (!schema) {
    return new Response('', { status: 404 });
  }

  const schemaText = await schema?.text();
  const schemaJSON = JSON.parse(schemaText);
  const parseResult = ParquetSchema.safeParse(schemaJSON);

  if (parseResult.success) {
    return new Response(JSON.stringify({ schema: parseResult.data }));
  }

  return new Response(JSON.stringify(parseResult.error), { status: 500 });
}

async function processPUTSchema(env: Env, request: Request) {
  const json = ParquetSchemaUpdateRequest.safeParse(await request.json<ParquetSchemaUpdateRequest>());
  if (!json.success) {
    return new Response(JSON.stringify(json.error), { status: 400 });
  }

  const putOperation = await env.LAKESIDE_BUCKET.put(`schema/schema.json`, JSON.stringify(json.data.schema));
  if (putOperation) {
    return new Response('Schema updated');
  } else {
    return new Response('Failed to commit schema update', { status: 500 });
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      if (request.method === 'GET') {
        return await processGETSchema(env);
      } else if (request.method === 'PUT') {
        return await processPUTSchema(env, request);
      }
      return new Response('', { status: 405 });
    } catch (e) {
      return new Response('failed to process request', { status: 500 });
    }
  },
};
