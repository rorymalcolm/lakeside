import { ParquetSchema, ParquetSchemaUpdateRequest } from 'parquet-types';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === 'GET') {
      const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
      const schemaJson = await schema?.text();
      const parseResult = ParquetSchema.safeParse(JSON.stringify(schemaJson));
      if (parseResult.success) {
        return new Response(JSON.stringify(parseResult.data));
      } else {
        return new Response(JSON.stringify(parseResult.error), { status: 500 });
      }
    } else if (request.method === 'PUT') {
      const json = ParquetSchemaUpdateRequest.safeParse(await request.json<ParquetSchemaUpdateRequest>());
      if (!json.success) {
        return new Response(JSON.stringify(json.error), { status: 400 });
      }
      const putOperation = await env.LAKESIDE_BUCKET.put(`schema/schema.json`, JSON.stringify(json.data));
      if (putOperation) {
        return new Response('OK');
      } else {
        return new Response('FAILED', { status: 500 });
      }
    }
    return new Response('', { status: 405 });
  },
};