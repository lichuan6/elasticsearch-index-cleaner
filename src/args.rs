use std::env;
use structopt::StructOpt;

/// Elasticsearch index cleaner
#[derive(StructOpt, Debug)]
#[structopt(name = "elasticsearch index cleaner")]
pub struct Opt {
    // A flag, true if used in the command line. Note doc comment will
    // be used for the help message of the flag. The name of the
    // argument will be, by default, based on the name of the field.
    /// Activate debug mode
    #[structopt(short, long)]
    pub debug: bool,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    pub verbose: u8,

    /// Elasticsearch address, or use ELASTICSEARCH_ADDR env
    #[structopt(short = "h", long)]
    pub elasticsearch_addr: Option<String>,

    /// Elasticsearch repository, or use ELASTICSEARCH_REPO env
    #[structopt(short = "r", long)]
    pub elasticsearch_repo: Option<String>,

    /// Elasticsearch indices to filter, comma separated list of index prefix
    ///
    /// i.e:
    ///  "istio-system-*,kong-*,kube-system-*,pulsar-*,logstash-*
    #[structopt(short = "f", long)]
    pub index_filter: Option<String>,

    /// How many days to keep the indices
    #[structopt(short = "k", long, default_value = "15")]
    pub keep_days: u32,
}

pub fn value_or_env(
    key: &str, other: Option<String>,
) -> anyhow::Result<String> {
    match other {
        Some(v) => Ok(v),
        None => match env::var(key) {
            Ok(v) => Ok(v),
            Err(e) => {
                let context = format!("{} must be set", key);
                Err(anyhow::Error::new(e).context(context))
            }
        },
    }
}

pub fn env_or(key: &str, other: Option<String>) -> anyhow::Result<String> {
    match env::var(key) {
        Ok(v) => Ok(v),
        Err(e) => match other {
            Some(v) => Ok(v),
            None => {
                let context = format!("{} must be set", key);
                Err(anyhow::Error::new(e).context(context))
            }
        },
    }
}
