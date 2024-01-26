use parquet::basic::{ConvertedType, Repetition, Type as PhysicalType};
use parquet::schema::printer;
use parquet::schema::types::Type;
use parquet::{file::writer::SerializedFileWriter, schema::parser::parse_message_type};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
struct ParquetSchema {
    fields: Vec<ParquetField>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ParquetField {
    name: String,
    #[serde(rename = "type")]
    primitive_type: ParquetPrimitiveType,
    logical_type: Option<ParquetLogicalType>,
    repetition_type: Option<ParquetRepetition>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ParquetPrimitiveType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Binary,
    Double,
    ByteArray,
    FixedLenByteArray,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ParquetLogicalType {
    Utf8,
    Map,
    MapKeyValue,
    List,
    Enum,
    Decimal,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Json,
    Bson,
    Interval,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ParquetRepetition {
    Required,
    Optional,
    Repeated,
}

fn logical_type_matcher(parquet_logical_type: ParquetLogicalType) -> ConvertedType {
    match parquet_logical_type {
        ParquetLogicalType::Utf8 => ConvertedType::UTF8,
        ParquetLogicalType::Map => ConvertedType::MAP,
        ParquetLogicalType::MapKeyValue => ConvertedType::MAP_KEY_VALUE,
        ParquetLogicalType::List => ConvertedType::LIST,
        ParquetLogicalType::Enum => ConvertedType::ENUM,
        ParquetLogicalType::Decimal => ConvertedType::DECIMAL,
        ParquetLogicalType::Date => ConvertedType::DATE,
        ParquetLogicalType::TimeMillis => ConvertedType::TIME_MILLIS,
        ParquetLogicalType::TimeMicros => ConvertedType::TIME_MICROS,
        ParquetLogicalType::TimestampMillis => ConvertedType::TIMESTAMP_MILLIS,
        ParquetLogicalType::TimestampMicros => ConvertedType::TIMESTAMP_MICROS,
        ParquetLogicalType::Uint8 => ConvertedType::UINT_8,
        ParquetLogicalType::Uint16 => ConvertedType::UINT_16,
        ParquetLogicalType::Uint32 => ConvertedType::UINT_32,
        ParquetLogicalType::Uint64 => ConvertedType::UINT_64,
        ParquetLogicalType::Int8 => ConvertedType::INT_8,
        ParquetLogicalType::Int16 => ConvertedType::INT_16,
        ParquetLogicalType::Int32 => ConvertedType::INT_32,
        ParquetLogicalType::Int64 => ConvertedType::INT_64,
        ParquetLogicalType::Json => ConvertedType::JSON,
        ParquetLogicalType::Bson => ConvertedType::BSON,
        ParquetLogicalType::Interval => ConvertedType::INTERVAL,
    }
}

fn physical_type_matcher(parquet_primitive_type: ParquetPrimitiveType) -> PhysicalType {
    match parquet_primitive_type {
        ParquetPrimitiveType::Boolean => PhysicalType::BOOLEAN,
        ParquetPrimitiveType::Int32 => PhysicalType::INT32,
        ParquetPrimitiveType::Int64 => PhysicalType::INT64,
        ParquetPrimitiveType::Int96 => PhysicalType::INT96,
        ParquetPrimitiveType::Binary => PhysicalType::BYTE_ARRAY,
        ParquetPrimitiveType::Double => PhysicalType::DOUBLE,
        ParquetPrimitiveType::ByteArray => PhysicalType::BYTE_ARRAY,
        ParquetPrimitiveType::FixedLenByteArray => PhysicalType::FIXED_LEN_BYTE_ARRAY,
    }
}

fn build_schema(schema: String) -> String {
    let schema = serde_json::from_str::<ParquetSchema>(schema.as_str()).unwrap();
    let mut type_vec: Vec<Arc<Type>> = vec![];
    for field in schema.fields {
        let type_builder = Type::primitive_type_builder(
            field.name.as_str(),
            physical_type_matcher(field.primitive_type),
        )
        .with_repetition(match field.repetition_type {
            Some(ParquetRepetition::Required) => Repetition::REQUIRED,
            Some(ParquetRepetition::Optional) => Repetition::OPTIONAL,
            Some(ParquetRepetition::Repeated) => Repetition::REPEATED,
            None => Repetition::REQUIRED,
        })
        .with_length(match field.primitive_type {
            ParquetPrimitiveType::FixedLenByteArray => 16,
            _ => 0,
        })
        .with_converted_type(match field.logical_type {
            Some(logical_type) => logical_type_matcher(logical_type),
            None => ConvertedType::NONE,
        });
        let converted_type = type_builder.build().unwrap();
        type_vec.push(Arc::new(converted_type));
    }
    let mut buf = Vec::new();
    let schema = Type::group_type_builder("schema")
        .with_fields(type_vec)
        .build()
        .unwrap();
    printer::print_schema(&mut buf, &schema);
    let string_schema = String::from_utf8(buf).unwrap();
    string_schema
}

#[wasm_bindgen]
pub fn generate_parquet(schema: String, fields: Vec<String>) -> Result<(), JsValue> {
    let message_type = build_schema(schema);

    let buffer = vec![];
    let schema = Arc::new(parse_message_type(message_type.as_str()).unwrap());
    let mut writer = SerializedFileWriter::new(buffer, schema, Default::default()).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    Ok(())
}

#[test]
fn test_build_schema() {
    let schema = r#"
    {
        "fields": [
            {
                "name": "id",
                "type": "INT32"
            },
            {
                "name": "name",
                "type": "BYTE_ARRAY",
                "logical_type": "UTF8"
            },
            {
                "name": "age",
                "type": "INT32"
            },
            {
                "name": "is_active",
                "type": "BOOLEAN"
            }
        ]
    }
    "#;
    let schema = build_schema(schema.to_string());
    assert_eq!(
        schema,
        "message schema {\n  REQUIRED INT32 id;\n  REQUIRED BYTE_ARRAY name (UTF8);\n  REQUIRED INT32 age;\n  REQUIRED BOOLEAN is_active;\n}\n"
    );
}
