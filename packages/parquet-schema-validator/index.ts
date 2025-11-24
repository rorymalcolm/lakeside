import { ParquetField, ParquetSchema } from 'parquet-types';
import { Result } from 'rerrors';

function validateJSONFieldAgainstSchema(key: string, JSONValue: any, field: ParquetField): Result {
  const errors: string[] = [];
  switch (field.type) {
    case 'BOOLEAN':
      if (typeof JSONValue !== 'boolean') {
        errors.push(`Field ${key} is not a boolean`);
      }
      break;
    case 'INT32':
      if (!Number.isInteger(JSONValue)) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case 'INT64':
      if (!Number.isInteger(JSONValue)) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case 'INT96':
      if (!Number.isInteger(JSONValue)) {
        errors.push(`Field ${key} is not an integer`);
      }
      break;
    case 'FLOAT':
      if (typeof JSONValue !== 'number' || isNaN(JSONValue)) {
        errors.push(`Field ${key} is not a float`);
      }
      break;
    case 'DOUBLE':
      if (typeof JSONValue !== 'number' || isNaN(JSONValue)) {
        errors.push(`Field ${key} is not a double`);
      }
      break;
    case 'BINARY':
      if (typeof JSONValue !== 'string') {
        errors.push(`Field ${key} is not a binary string`);
      }
      break;
    case 'BYTE_ARRAY':
      if (typeof JSONValue !== 'string') {
        errors.push(`Field ${key} is not a string`);
      }
      break;
    case 'FIXED_LEN_BYTE_ARRAY':
      if (typeof JSONValue !== 'string') {
        errors.push(`Field ${key} is not a string`);
      }
      break;
    default:
      errors.push(`Unknown type ${field.type}`);
  }

  if (field.logicalType) {
    switch (field.logicalType) {
      case 'UTF8':
        if (typeof JSONValue !== 'string') {
          errors.push(`Field ${key} is not a string`);
        }
        break;
      case 'MAP':
        if (typeof JSONValue !== 'object') {
          errors.push(`Field ${key} is not an object`);
        }
        break;
      case 'MAP_KEY_VALUE':
        if (typeof JSONValue !== 'object') {
          errors.push(`Field ${key} is not an object`);
        }
        break;
      case 'LIST':
        if (!Array.isArray(JSONValue)) {
          errors.push(`Field ${key} is not an array`);
        }
        break;
      case 'ENUM':
        if (typeof JSONValue !== 'string') {
          errors.push(`Field ${key} is not a string`);
        }
        break;
      case 'DECIMAL':
        if (typeof JSONValue !== 'string') {
          errors.push(`Field ${key} is not a string decimal`);
        }
        break;
      case 'DATE':
        if (typeof JSONValue !== 'string' || isNaN(Date.parse(JSONValue))) {
          errors.push(`Field ${key} is not a date`);
        }
        break;
      case 'TIME_MILLIS':
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'TIME_MICROS':
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'TIMESTAMP_MILLIS':
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'TIMESTAMP_MICROS':
        if (!Number.isInteger(Number(JSONValue))) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'INT_8':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'INT_16':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'INT_32':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'INT_64':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'UINT_8':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'UINT_16':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'UINT_32':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'UINT_64':
        if (!Number.isInteger(JSONValue)) {
          errors.push(`Field ${key} is not an integer`);
        }
        break;
      case 'JSON':
        if (typeof JSONValue !== 'object') {
          errors.push(`Field ${key} is not a JSON object`);
        }
        break;
      case 'BSON':
        if (typeof JSONValue !== 'object') {
          errors.push(`Field ${key} is not a BSON object`);
        }
        break;
      case 'INTERVAL':
        if (typeof JSONValue !== 'string') {
          errors.push(`Field ${key} is not an interval string`);
        }
        break;
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

export function validateJSONAgainstSchema(json: any, schema: ParquetSchema): Result {
  if (typeof json !== 'object' || json === null) {
    return {
      success: false,
      errors: ['JSON is not a valid object'],
    };
  }

  for (const field of schema.fields) {
    const errors = [];
    const fieldValue = json[field.name];

    // Only validate required fields if they're missing
    if (fieldValue === undefined || fieldValue === null) {
      if (field.repetitionType === 'REQUIRED') {
        errors.push(`Missing required field ${field.name}`);
      }
    }

    if (errors.length > 0) {
      return {
        success: false,
        errors,
      };
    }

    // Skip validation if field is optional and missing
    if ((fieldValue === undefined || fieldValue === null) && field.repetitionType !== 'REQUIRED') {
      continue;
    }

    const validated = validateJSONFieldAgainstSchema(field.name, fieldValue, field);
    if (validated.success === false) {
      return validated;
    }
  }

  for (const key of Object.keys(json)) {
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
