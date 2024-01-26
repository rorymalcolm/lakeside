import { v4 } from 'uuid';

export interface Env {
  LAKESIDE_BUCKET: R2Bucket;
}

type AppendBody = {
  body: string;
};

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      if (request.method === 'PUT') {
        const json = await request.json<AppendBody>();

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
