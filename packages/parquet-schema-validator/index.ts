import { ParquetField, ParquetSchema } from "parquet-types";

const safeParse = (json: string) => {
  try {
    return {
      success: true,
      data: JSON.parse(json),
    };
  } catch (e) {
    return {
      success: false,
      error: "Unable to parse JSON",
    };
  }
};

function validateJSONFieldAgainstSchmea(
  key: string,
  JSONValue: string,
  field: ParquetField
):
  | {
      success: true;
    }
  | {
      success: false;
      errors: string[];
    } {
  const errors: string[] = [];
  switch (field.type) {
    case "BOOLEAN":
      if (JSONValue !== "true" && JSONValue !== "false") {
        errors.push(`Field ${key} is not a boolean`);
      }
      break;
    case "INT32":
      if (!Number.isInteger(Number(JSONValue))) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case "INT64":
      if (!Number.isInteger(Number(JSONValue))) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case "INT96":
      if (!Number.isInteger(Number(JSONValue))) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case "FLOAT":
      if (isNaN(Number(JSONValue))) {
        errors.push(`Field ${key} is not a float`);
      }
      break;
    case "DOUBLE":
      if (isNaN(Number(JSONValue))) {
        errors.push(`Field ${key} is not a double`);
      }
      break;
    case "BYTE_ARRAY":
      if (typeof JSONValue !== "string") {
        errors.push(`Field ${key} is not a string`);
      }
      break;
    case "FIXED_LEN_BYTE_ARRAY":
      if (typeof JSONValue !== "string") {
        errors.push(`Field ${key} is not a string`);
      }
      break;
    default:
      errors.push(`Unknown type ${field.type}`);
  }

  if (field.logicalType) {
    switch (field.logicalType) {
      case "UTF8":
        if (typeof JSONValue !== "string") {
          errors.push(`Field ${key} is not a string`);
        }
        break;
      case "MAP":
        if (typeof JSONValue !== "object") {
          errors.push(`Field ${key} is not an object`);
        }
        break;
      case "MAP_KEY_VALUE":
        if (typeof JSONValue !== "object") {
          errors.push(`Field ${key} is not an object`);
        }
        break;
      case "LIST":
        if (!Array.isArray(JSONValue)) {
          errors.push(`Field ${key} is not an array`);
        }
        break;
      case "ENUM":
        if (typeof JSONValue !== "string") {
          errors.push(`Field ${key} is not a string`);
        }
        break;
      case "DECIMAL":
        if (typeof JSONValue !== "string") {
          errors.push(`Field ${key} is not a string decimal`);
        }
        break;
      case "DATE":
        if (typeof JSONValue !== "string" || isNaN(Date.parse(JSONValue))) {
          errors.push(`Field ${key} is not a date`);
        }
        break;
      case "TIME_MILLIS":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "TIME_MICROS":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "TIMESTAMP_MILLIS":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "TIMESTAMP_MICROS":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "UINT_8":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "UINT_16":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case "UINT_32":
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
    }
  }
  if (errors.length > 0) {
    return {
      success: false,
      errors,
    };
  }
  return {
    success: true,
  };
}

export function validateJSONAgainstSchema(
  json: string,
  schema: ParquetSchema
):
  | {
      success: false;
      errors: string[];
    }
  | {
      success: true;
    } {
  const parsedJSON = safeParse(json);
  if (!parsedJSON.success) {
    return {
      success: false,
      errors: ["JSON is not valid JSON"],
    };
  }

  for (const field of schema.fields) {
    const errors = [];
    if (!parsedJSON.data[field.name]) {
      errors.push(`Missing field ${field.name}`);
    }
    if (errors.length > 0) {
      return {
        success: false,
        errors,
      };
    }
    const validated = validateJSONFieldAgainstSchmea(
      field.name,
      parsedJSON.data[field.name],
      field
    );
    if (validated.success === false) {
      return validated;
    }
  }

  for (const key of Object.keys(parsedJSON.data)) {
    const errors = [];
    if (!schema.fields.find((field) => field.name === key)) {
      errors.push(`Unknown field ${key}`);
    }
    if (errors.length > 0) {
      return {
        success: false,
        errors,
      };
    }
  }

  return {
    success: true,
  };
}
