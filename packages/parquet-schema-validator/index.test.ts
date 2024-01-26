import { describe, expect, it } from "vitest";
import { validateJSONAgainstSchema } from ".";
import { ParquetSchema } from "parquet-types";

describe("validateJSONAgainstSchema", () => {
  it("should reject non json input", () => {
    const invalidJSON = "this is not json";
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(invalidJSON, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["JSON is not valid JSON"]);
  });

  it("should reject missing fields", () => {
    const json = JSON.stringify({});
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["Missing field test"]);
  });

  it("should reject unknown fields", () => {
    const json = JSON.stringify({ test: 1, unknown: 2 });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["Unknown field unknown"]);
  });

  it("should reject invalid field types", () => {
    const json = JSON.stringify({ test: "not a number" });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["Field test is not an integer"]);
  });

  it("should accept valid json", () => {
    const json = JSON.stringify({ test: 1 });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(true);
  });

  it("should accept valid json with multiple fields", () => {
    const json = JSON.stringify({ test: 1, test2: 2 });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
        {
          name: "test2",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(true);
  });

  it("should reject invalid field types with multiple fields", () => {
    const json = JSON.stringify({ test: 1, test2: "not a number" });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "INT64",
        },
        {
          name: "test2",
          type: "INT64",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["Field test2 is not an integer"]);
  });

  it("should accept string field types", () => {
    const json = JSON.stringify({ test: "a string" });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "BYTE_ARRAY",
          logicalType: "UTF8",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(true);
  });

  it("should reject non timestamp logical types", () => {
    const json = JSON.stringify({ test: "a string" });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "BYTE_ARRAY",
          logicalType: "DATE",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(false);
    expect(result).toHaveProperty("errors");
    if (result.success) {
      throw new Error("should not be successful");
    }
    expect(result.errors).toEqual(["Field test is not a date"]);
  });

  it("should accept timestamp logical types", () => {
    const json = JSON.stringify({ test: "2021-01-01T00:00:00.000Z" });
    const schema: ParquetSchema = {
      fields: [
        {
          name: "test",
          type: "BYTE_ARRAY",
          logicalType: "DATE",
        },
      ],
    };
    const result = validateJSONAgainstSchema(json, schema);
    expect(result.success).toBe(true);
  });
});
