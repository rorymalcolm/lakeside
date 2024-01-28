import { wasm_test } from 'wasm-test-rs';

export interface Env {
}


export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const wasmRes = wasm_test();
    return new Response(wasmRes);
  },
};
