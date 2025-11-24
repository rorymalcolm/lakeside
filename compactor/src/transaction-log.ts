/**
 * Delta Lake-style transaction log for ACID guarantees
 *
 * Structure:
 * _lakeside_log/
 *   00000000.json  <- First transaction
 *   00000001.json  <- Second transaction
 *   ...
 */

export interface TransactionEntry {
  version: number;
  timestamp: string;
  operation: 'compact' | 'schema_change' | 'cleanup';
  add?: FileAction[];
  remove?: FileAction[];
  metadata?: Record<string, any>;
}

export interface FileAction {
  path: string;
  size?: number;
  rowCount?: number;
  partition?: string;
}

export interface Partition {
  key: string;           // e.g., "order_ts_hour=2025-11-23T19"
  fileKeys: string[];    // Source JSON files
  rowCount: number;
}

/**
 * Extract partition key from file path
 * Input:  "data/order_ts_hour=2025-11-23T19/abc123.json"
 * Output: "order_ts_hour=2025-11-23T19"
 */
export function extractPartition(filePath: string): string | null {
  const match = filePath.match(/data\/([^/]+)\//);
  return match ? match[1] : null;
}

/**
 * Group files by partition
 */
export function groupFilesByPartition(fileKeys: string[]): Map<string, string[]> {
  const partitions = new Map<string, string[]>();

  for (const key of fileKeys) {
    const partition = extractPartition(key);
    if (partition) {
      const existing = partitions.get(partition) || [];
      existing.push(key);
      partitions.set(partition, existing);
    }
  }

  return partitions;
}

/**
 * Get the next transaction version by reading the log
 */
export async function getNextVersion(bucket: R2Bucket): Promise<number> {
  const list = await bucket.list({ prefix: '_lakeside_log/' });

  if (list.objects.length === 0) {
    return 0;
  }

  // Find highest version number
  const versions = list.objects
    .map(obj => {
      const match = obj.key.match(/_lakeside_log\/(\d+)\.json/);
      return match ? parseInt(match[1], 10) : -1;
    })
    .filter(v => v >= 0);

  return versions.length > 0 ? Math.max(...versions) + 1 : 0;
}

/**
 * Append a transaction to the log (atomic write with conditional put)
 */
export async function appendTransaction(
  bucket: R2Bucket,
  transaction: Omit<TransactionEntry, 'version'>
): Promise<number> {
  const version = await getNextVersion(bucket);

  const entry: TransactionEntry = {
    version,
    ...transaction,
  };

  const key = `_lakeside_log/${version.toString().padStart(8, '0')}.json`;

  // Use conditional write to prevent concurrent writes creating duplicate versions
  try {
    await bucket.put(key, JSON.stringify(entry, null, 2), {
      httpMetadata: {
        contentType: 'application/json',
      },
      onlyIf: {
        // Only write if file doesn't exist (etagDoesNotMatch: '*' means "no ETag exists")
        etagDoesNotMatch: '*',
      },
    });
  } catch (error) {
    // If conditional write fails, another compaction wrote this version
    // Retry with next version number
    console.warn(`Version conflict at ${version}, retrying...`);
    return appendTransaction(bucket, transaction);
  }

  return version;
}

/**
 * Read all transactions from the log
 */
export async function readTransactionLog(bucket: R2Bucket): Promise<TransactionEntry[]> {
  const list = await bucket.list({ prefix: '_lakeside_log/' });

  const transactions: TransactionEntry[] = [];

  for (const obj of list.objects) {
    const content = await bucket.get(obj.key);
    if (content) {
      const text = await content.text();
      transactions.push(JSON.parse(text));
    }
  }

  // Sort by version
  transactions.sort((a, b) => a.version - b.version);

  return transactions;
}

/**
 * Get current state by replaying transaction log
 * Returns set of active parquet files and list of files to clean up
 */
export async function getCurrentState(bucket: R2Bucket): Promise<{
  parquetFiles: Set<string>;
  orphanedJsonFiles: Set<string>;
}> {
  const transactions = await readTransactionLog(bucket);

  const parquetFiles = new Set<string>();
  const removedFiles = new Set<string>();

  for (const tx of transactions) {
    // Add files
    if (tx.add) {
      for (const file of tx.add) {
        parquetFiles.add(file.path);
      }
    }

    // Track removed files
    if (tx.remove) {
      for (const file of tx.remove) {
        removedFiles.add(file.path);
      }
    }
  }

  // List actual JSON files in bucket
  const actualJsonFiles = new Set<string>();
  const jsonList = await bucket.list({ prefix: 'data/' });
  for (const obj of jsonList.objects) {
    actualJsonFiles.add(obj.key);
  }

  // Orphaned = files that should have been deleted but still exist
  const orphanedJsonFiles = new Set<string>();
  for (const file of removedFiles) {
    if (actualJsonFiles.has(file)) {
      orphanedJsonFiles.add(file);
    }
  }

  return { parquetFiles, orphanedJsonFiles };
}
