use elasticsearch_index_cleaner::args::Opt;
use elasticsearch_index_cleaner::es;
use elasticsearch_index_cleaner::es::indices_clean;
use std::env;
use structopt::StructOpt;

pub fn env_or_else(key: &str, or: &str) -> String {
    env::var(key).ok().unwrap_or_else(|| or.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Elasticsearch index cleaner started!");
    let opt = Opt::from_args();
    let es_addr = env_or_else("ELASTICSEARCH_ADDRESS", &opt.elasticsearch_addr);
    let repository = env_or_else("ELASTICSEARCH_REPOSITORY", &opt.elasticsearch_repo);
    let keep_days = opt.keep_days;
    let index_filter = env_or_else("ELASTICSEARCH_INDEX_FILTER", &opt.index_filter);

    let client = es::create_client(&es_addr).unwrap();
    indices_clean(&client, &repository, keep_days, &index_filter).await?;

    Ok(())
}
