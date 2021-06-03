use elasticsearch_index_cleaner::{
    args::{value_or_env, Opt},
    es,
    es::indices_clean,
};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Elasticsearch index cleaner started!");
    let opt = Opt::from_args();

    let es_addr = value_or_env("ELASTICSEARCH_ADDR", opt.elasticsearch_addr)?;
    let repository =
        value_or_env("ELASTICSEARCH_REPO", opt.elasticsearch_repo)?;
    let index_filter =
        value_or_env("ELASTICSEARCH_INDEX_FILTER", opt.index_filter)?;
    let keep_days = opt.keep_days;

    let client = es::create_client(&es_addr).unwrap();
    indices_clean(&client, &repository, keep_days, &index_filter).await?;

    Ok(())
}
