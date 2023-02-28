use aws_lambda_events::s3::S3Event;
use flate2::read::GzDecoder;
use flate2::Compression;
use flate2::GzBuilder;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::Read;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    let func = service_fn(move |req: LambdaEvent<S3Event>| {
        handler(s3_client.clone(), req.payload, req.context)
    });
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(
    s3_client: aws_sdk_s3::Client,
    events: S3Event,
    _ctx: lambda_runtime::Context,
) -> Result<(), Box<Error>> {
    for e in events.records {
        let bucket_name = e.s3.bucket.name.expect("Unable to get s3 bucket name.");
        let key = e.s3.object.key.expect("unable to get s3 file key");

        let data = s3_client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();

        let mut d = GzDecoder::new(&data[..]);
        let mut csv_data = String::new();
        d.read_to_string(&mut csv_data).unwrap();

        let split = csv_data.lines();
        let result_vector = split.collect::<Vec<_>>();

        let mut tab_converted = String::new();
        for line in result_vector.iter().skip(1) {
            let date = &line[0..14].trim();
            let serial_number = &line[15..35].trim();
            let model = &line[36..78].trim();
            let capacity_bytes = &line[79..97].trim();
            let failure = &line[98..108].trim();
            let tab_line = format!(
                "{}\t{}\t{}\t{}\t{}\n",
                date, serial_number, model, capacity_bytes, failure
            );
            tab_converted.push_str(&tab_line);
        }
        let f = File::create("/tmp/file.gz").expect("failed to create file");
        let mut gz = GzBuilder::new()
            .filename("tab_converted.txt")
            .write(f, Compression::default());
        gz.write_all(tab_converted.as_bytes())
            .expect("failed to write bytes to file");
        gz.finish().expect("failed to flush bytes to file");

        let file = File::open("/tmp/file.gz").expect("problem reading file");
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();

        reader.read_to_end(&mut buffer).expect("error");

        let remote_uri = &key.replace("fixed_width_raw/", "tab_converted/");
        s3_client
            .put_object()
            .bucket(&bucket_name)
            .key(remote_uri)
            .body(buffer.into())
            .send()
            .await
            .unwrap();
    }
    Ok(())
}
