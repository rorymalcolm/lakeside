import { v4 } from 'uuid';
import { validateJSONAgainstSchema } from 'parquet-schema-validator';
import { ParquetSchema } from 'parquet-types';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

type AppendBody = {
  body: string;
};

const getSchema = async (
  env: Env
): Promise<
  | {
      success: true;
      schema: ParquetSchema;
    }
  | {
      success: false;
      error: string;
    }
> => {
  const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
  if (!schema) {
    return {
      success: false,
      error: 'No schema',
    };
  }

  const schemaText = await schema?.text();
  const schemaJSON = JSON.parse(schemaText);
  const parseResult = ParquetSchema.safeParse(schemaJSON);

  if (parseResult.success) {
    return {
      success: true,
      schema: parseResult.data,
    };
  }

  return {
    success: false,
    error: JSON.stringify(parseResult.error),
  };
};

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const schemaResult = await getSchema(env);
    if (!schemaResult.success) {
      return new Response(schemaResult.error, { status: 500 });
    }

    try {
      if (request.method === 'PUT') {
        const json = await request.json<AppendBody>();

        const validated = validateJSONAgainstSchema(json.body, schemaResult.schema);

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
      return new Response('', { status: 405 });
    } catch (e) {
      return new Response('failed to process request', { status: 500 });
    }
  },
};
