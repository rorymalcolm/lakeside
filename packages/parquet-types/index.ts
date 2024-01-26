import { z } from "zod";

const ParquetRepetitionType = z.enum(["REQUIRED", "OPTIONAL", "REPEATED"]);

type ParquetRepetitionType = z.infer<typeof ParquetRepetitionType>;

const ParquetPrimitiveType = z.enum([
  "BOOLEAN",
  "INT32",
  "INT64",
  "INT96",
  "FLOAT",
  "DOUBLE",
  "BINARY",
  "BYTE_ARRAY",
  "FIXED_LEN_BYTE_ARRAY",
]);

type ParquetPrimitiveType = z.infer<typeof ParquetPrimitiveType>;

const ParquetLogicalType = z.enum([
  "UTF8",
  "MAP",
  "MAP_KEY_VALUE",
  "LIST",
  "ENUM",
  "DECIMAL",
  "DATE",
  "TIME_MILLIS",
  "TIME_MICROS",
  "TIMESTAMP_MILLIS",
  "TIMESTAMP_MICROS",
  "UINT_8",
  "UINT_16",
  "UINT_32",
  "UINT_64",
  "INT_8",
  "INT_16",
  "INT_32",
  "INT_64",
  "JSON",
  "BSON",
  "INTERVAL",
]);

type ParquetLogicalType = z.infer<typeof ParquetLogicalType>;

export const ParquetSchema = z.object({
  fields: z.array(
    z.object({
      name: z.string(),
      type: ParquetPrimitiveType,
      logicalType: ParquetLogicalType.optional(),
      repetitionType: ParquetRepetitionType.optional(),
    })
  ),
});

export type ParquetSchema = z.infer<typeof ParquetSchema>;

export const ParquetSchemaUpdateRequest = z.object({
  schema: ParquetSchema,
});

export type ParquetSchemaUpdateRequest = z.infer<
  typeof ParquetSchemaUpdateRequest
>;
