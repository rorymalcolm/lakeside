use std::{env, sync::Arc};

use parquet::arrow;
use s3::creds::Credentials;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn generate_parquet(input_json: &str, s3_key: &str) {
    //write a noop file to the s3 bucket
    let creds = Credentials::new(
        Option::from(env::var("CLOUDFLARE_R2_ACCESS_KEY").unwrap()),
        Option::from(env::var("CLOUDFLARE_R2_SECRET_KEY").unwrap()),
        None,
        None,
        None,
    )
    .unwrap();
    let cloudflare_account_id = env::var("CLOUDFLARE_ACCOUNT_ID").unwrap();
    let region = Region::Custom {
        region: "us-east-1",
        endpoint: format!("https://{cloudflare_account_id}.r2.cloudflarestorage.com"),
    };
    let s3_client = Arc::new(s3::S3Client::new(creds, region));
    let bucket = env::var("CLOUDFLARE_BUCKET").unwrap();
    s3_client.put_object(&bucket, s3_key, &[]).unwrap();
}
