use std::ops::Range;
use std::{env, sync::Arc};

use anyhow::Context as _;
use futures::{StreamExt as _, TryStreamExt as _};

const CONCURRENCY_LIMIT: usize = 500;
const RANGE: Range<u32> = 17_000_000..17_002_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let aws_config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&aws_config);

    let bucket_name = env::var("AWS_BUCKET_NAME").context("AWS_BUCKET_NAME not found in env")?;
    let bucket_name = Arc::new(bucket_name);

    let results = futures::stream::iter(RANGE)
        .map(move |object_id| {
            let s3_client = s3_client.clone();
            let bucket_name = bucket_name.clone();

            async move {
                let response = s3_client
                    .get_object()
                    .bucket(&*bucket_name)
                    .key(format!("{object_id}.txt"))
                    .send()
                    .await
                    .with_context(|| format!("Failed to fetch object {object_id}"))?;

                let object_bytes = response
                    .body
                    .collect()
                    .await
                    .map(|data| data.into_bytes())
                    .with_context(|| format!("Failed to collect bytes of object {object_id}"))?;

                anyhow::Ok((object_id, object_bytes))
            }
        })
        .buffered(CONCURRENCY_LIMIT)
        .inspect_ok(|(object_id, object_bytes)| {
            println!("Fetched {object_id}, bytes.len: {}", object_bytes.len())
        })
        .try_collect::<Vec<_>>()
        .await?;

    println!("results: {results:?}");

    Ok(())
}
