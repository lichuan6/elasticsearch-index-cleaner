use structopt::StructOpt;

/// An elasticsearch snapshots management tool
#[derive(StructOpt, Debug)]
#[structopt(name = "elasticsearch snapshot")]
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

    /// Elasticsearch address
    #[structopt(short = "h", long, default_value = "http://localhost:9200")]
    pub elasticsearch_addr: String,

    /// Elasticsearch repository
    #[structopt(short = "r", long)]
    pub elasticsearch_repo: String,

    /// Elasticsearch indices to filter, comma separated list of index prefix,
    /// i.e app-,logstash-
    #[structopt(
        short = "f",
        long,
        default_value = "app-*,istio-system-*,kong-*,kube-system-*,pulsar-*,\
                         logstash-*,haproxy-*,nginx-*,eksfan-logstash-*,\
                         kong-logstash-*,kongingress-*,app-meican-logstash-*"
    )]
    pub index_filter: String,

    /// Elasticsearch days to keep
    #[structopt(short = "k", long, default_value = "15")]
    pub keep_days: u32,
}
