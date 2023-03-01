use std::io::prelude::*;
use std::io::BufReader;
use std::io::Read;

use anyhow::Context;
use anyhow::{anyhow, Result};
use aws_lambda_events::s3::S3Event;
use flate2::read::GzDecoder;
use flate2::Compression;
use flate2::GzBuilder;
use lambda_runtime::{service_fn, LambdaEvent};
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;
use tokio::task;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    let func = service_fn(move |req: LambdaEvent<S3Event>| {
        handler(s3_client.clone(), req.payload, req.context)
    });
    lambda_runtime::run(func).await.map_err(|e| anyhow!(e))?;
    Ok(())
}

struct ReadFromAsync<T: AsyncRead>(T);

impl<T> Read for ReadFromAsync<T>
where
    T: AsyncRead + Unpin,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // T is an async read, so we pivot to async
        Handle::current().block_on(async move { self.0.read(buf).await })
    }
}

async fn handler(
    s3_client: aws_sdk_s3::Client,
    events: S3Event,
    _ctx: lambda_runtime::Context,
) -> Result<()> {
    for e in events.records {
        let bucket_name = e.s3.bucket.name.context("Unable to get s3 bucket name.")?;
        let key = e.s3.object.key.context("unable to get s3 file key")?;

        let data = s3_client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await?
            .body
            .into_async_read();

        let tab_converted = task::block_in_place(move || {
            let d = BufReader::new(GzDecoder::new(ReadFromAsync(data)));
            let split = d.lines();
            let gz_output = vec![];
            let mut gz = GzBuilder::new()
                .filename("tab_converted.txt")
                .write(gz_output, Compression::new(6));

            for line in split.skip(1) {
                let line = line?;
                let date = &line[0..14].trim();
                let serial_number = &line[15..35].trim();
                let model = &line[36..78].trim();
                let capacity_bytes = &line[79..97].trim();
                let failure = &line[98..108].trim();
                writeln!(
                    gz,
                    "{}\t{}\t{}\t{}\t{}",
                    date, serial_number, model, capacity_bytes, failure
                )?;
            }
            gz.finish()
        })?;

        let remote_uri = &key.replace("fixed_width_raw/", "tab_converted/");
        s3_client
            .put_object()
            .bucket(&bucket_name)
            .key(remote_uri)
            .body(tab_converted.into())
            .send()
            .await
            .context("failed to to upload result")?;
    }
    Ok(())
}
