use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;

#[wasm_bindgen]
pub fn wasm_test() -> Result<JsValue, JsValue> {
    Ok(JsValue::from("Hello from Rust!".to_string()))
}
