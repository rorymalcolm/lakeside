// Durable Object for distributed compaction locking
interface CompactionState {
  isCompacting: boolean;
  currentBatch?: string[];
  startedAt?: number;
}

/**
 * CompactionCoordinator ensures only one compaction runs at a time
 * across all worker instances using Durable Objects for distributed locking.
 */
export class CompactionCoordinator implements DurableObject {
  private state: CompactionState = {
    isCompacting: false,
  };

  private ctx: DurableObjectState;
  private env: unknown;

  constructor(ctx: DurableObjectState, env: unknown) {
    this.ctx = ctx;
    this.env = env;
    this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get<CompactionState>('state');
      if (stored) {
        this.state = stored;

        // Recovery: If compaction was running for >10 minutes, assume it crashed
        if (this.state.isCompacting && this.state.startedAt) {
          const elapsed = Date.now() - this.state.startedAt;
          if (elapsed > 10 * 60 * 1000) {
            console.warn(`Compaction lock expired after ${elapsed}ms - resetting`);
            this.state.isCompacting = false;
            this.state.currentBatch = undefined;
            this.state.startedAt = undefined;
          }
        }
      }
    });
  }

  /**
   * Try to acquire the compaction lock with the list of files to compact.
   * Returns true if lock was acquired, false if another compaction is running.
   */
  async tryAcquire(fileKeys: string[]): Promise<boolean> {
    if (this.state.isCompacting) {
      return false; // Another compaction is already running
    }

    this.state = {
      isCompacting: true,
      currentBatch: fileKeys,
      startedAt: Date.now(),
    };

    await this.ctx.storage.put('state', this.state);
    return true;
  }

  /**
   * Release the compaction lock after successful completion.
   */
  async release(): Promise<void> {
    this.state = {
      isCompacting: false,
      currentBatch: undefined,
      startedAt: undefined,
    };

    await this.ctx.storage.put('state', this.state);
  }

  /**
   * Get the current compaction state (for monitoring/debugging).
   */
  async getState(): Promise<CompactionState> {
    return { ...this.state };
  }

  /**
   * Force release the lock (emergency use only - e.g., if compaction crashed).
   */
  async forceRelease(): Promise<void> {
    console.warn('Force releasing compaction lock');
    await this.release();
  }

  /**
   * Handle HTTP requests to the Durable Object.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    switch (url.pathname) {
      case '/acquire': {
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 });
        }

        const fileKeys = await request.json() as string[];
        const acquired = await this.tryAcquire(fileKeys);

        if (acquired) {
          return new Response(JSON.stringify({ success: true }), { status: 200 });
        } else {
          return new Response(
            JSON.stringify({
              success: false,
              message: `Compaction in progress: ${this.state.currentBatch?.length || 0} files, started ${
                this.state.startedAt ? new Date(this.state.startedAt).toISOString() : 'unknown'
              }`,
            }),
            { status: 409 }
          );
        }
      }

      case '/release': {
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 });
        }

        await this.release();
        return new Response(JSON.stringify({ success: true }), { status: 200 });
      }

      case '/status': {
        if (request.method !== 'GET') {
          return new Response('Method not allowed', { status: 405 });
        }

        const state = await this.getState();
        return new Response(JSON.stringify(state), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      case '/force-release': {
        if (request.method !== 'POST') {
          return new Response('Method not allowed', { status: 405 });
        }

        await this.forceRelease();
        return new Response(JSON.stringify({ success: true }), { status: 200 });
      }

      default:
        return new Response('Not found', { status: 404 });
    }
  }
}
