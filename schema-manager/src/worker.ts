import { z } from 'zod';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

type SchemaRequest = {
  schema: ParquetSchema;
};

type ParquetSchema = {
  fields: ParquetSchemaField[];
};

type ParquetSchemaField = {
  name: string;
  type: ParquetPrimitiveType;
  logicalType?: ParquetLogicalType;
  repetitionType?: ParquetRepeitionType;
};

const ParquetRepetitionType = z.enum(['REQUIRED', 'OPTIONAL', 'REPEATED']);

type ParquetRepeitionType = z.infer<typeof ParquetRepetitionType>;

const ParquetPrimitiveType = z.enum([
  'BOOLEAN',
  'INT32',
  'INT64',
  'INT96',
  'FLOAT',
  'DOUBLE',
  'BINARY',
  'BYTE_ARRAY',
  'FIXED_LEN_BYTE_ARRAY',
]);

type ParquetPrimitiveType = z.infer<typeof ParquetPrimitiveType>;

const ParquetLogicalType = z.enum([
  'UTF8',
  'MAP',
  'MAP_KEY_VALUE',
  'LIST',
  'ENUM',
  'DECIMAL',
  'DATE',
  'TIME_MILLIS',
  'TIME_MICROS',
  'TIMESTAMP_MILLIS',
  'TIMESTAMP_MICROS',
  'UINT_8',
  'UINT_16',
  'UINT_32',
  'UINT_64',
  'INT_8',
  'INT_16',
  'INT_32',
  'INT_64',
  'JSON',
  'BSON',
  'INTERVAL',
]);

type ParquetLogicalType = z.infer<typeof ParquetLogicalType>;

const schemaRequest = z.object({
  fields: z.array(
    z.object({
      name: z.string(),
      type: ParquetPrimitiveType,
      logicalType: ParquetLogicalType.optional(),
      repetitionType: ParquetRepetitionType.optional(),
    })
  ),
});

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === 'GET') {
      const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
      const schemaJson = await schema?.text();
      const parseResult = schemaRequest.safeParse(JSON.stringify(schemaJson));
      if (parseResult.success) {
        return new Response(JSON.stringify(parseResult.data));
      } else {
        return new Response(JSON.stringify(parseResult.error), { status: 500 });
      }
    } else if (request.method === 'PUT') {
      const json = schemaRequest.safeParse(await request.json<SchemaRequest>());
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
