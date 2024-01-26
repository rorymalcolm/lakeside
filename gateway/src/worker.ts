import { v4 } from 'uuid';
import { validateJSONAgainstSchema } from 'parquet-schema-validator';
import { ParquetSchema } from 'parquet-types';
import { ValueResult, ErrorsToResponse } from 'rerrors';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

type AppendBody = {
  body: string;
};

const getSchema = async (env: Env): Promise<ValueResult<ParquetSchema>> => {
  const schema = await env.LAKESIDE_BUCKET.get(`schema/schema.json`);
  if (!schema) {
    return {
      success: false,
      errors: ['No schema'],
    };
  }

  const schemaText = await schema?.text();
  const schemaJSON = JSON.parse(schemaText);
  const parseResult = ParquetSchema.safeParse(schemaJSON);

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
      if (request.method === 'PUT') {
        const json = await request.json<AppendBody>();

        const validated = validateJSONAgainstSchema(json.body, schemaResult.value);

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
