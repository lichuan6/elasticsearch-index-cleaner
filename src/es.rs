use crate::date;
use chrono::{DateTime, Utc};
use elasticsearch::{
    cat::CatIndicesParts,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::IndicesDeleteParts,
    snapshot::{SnapshotCreateParts, SnapshotStatusParts},
    Elasticsearch, Error,
};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use url::Url;

#[derive(Deserialize, Debug)]
struct IndexAndCreationDate {
    /// create_date
    #[serde(with = "date", rename(deserialize = "cd"))]
    creation_date: DateTime<Utc>,
    /// index name
    #[serde(rename(deserialize = "i"))]
    index: String,
}

#[derive(Deserialize, Debug)]
struct Snapshots {
    snapshots: Vec<Snapshot>,
}

#[derive(Deserialize, Debug)]
struct Snapshot {
    snapshot: String,
    state: String,
}

/// Create a Elasticsearch client
pub fn create_client(addr: &str) -> anyhow::Result<Elasticsearch, Error> {
    let url = Url::parse(addr)?;

    let conn_pool = SingleNodeConnectionPool::new(url);
    let builder = TransportBuilder::new(conn_pool);

    let transport = builder.build()?;
    Ok(Elasticsearch::new(transport))
}

/// Clean elasticsearch indices, take snapshots for outdated indices, and delete
/// the coresponding indices after snapshots are successfully created.
pub async fn indices_clean(
    client: &Elasticsearch, repository: &str, keep_days: u32,
    index_filter: &str,
) -> anyhow::Result<()> {
    let index_filter = index_filter.split(',').collect::<Vec<_>>();
    let outdated_indices =
        get_outdated_indices(client, keep_days, &index_filter).await?;
    if !outdated_indices.is_empty() {
        log::info!("{} outdated indices found", &outdated_indices.len());
    }
    for index in outdated_indices {
        take_snapshot_and_check(client, repository, &index).await?;
        delete_index(client, &index).await?;
    }
    Ok(())
}

/// Return a vector of outdated indices
async fn get_outdated_indices(
    client: &Elasticsearch, keep_days: u32, index_filter: &[&str],
) -> anyhow::Result<Vec<String>> {
    let response = client
        .cat()
        .indices(CatIndicesParts::Index(index_filter))
        // h: Comma-separated list of column names to display
        // cd means creation.date, i mean index
        .h(&["cd", "i"])
        .format("json")
        .send()
        .await?;

    log::debug!("calling cat indices response : {:?}", response);
    let indices: Vec<IndexAndCreationDate> =
        response.json::<Vec<IndexAndCreationDate>>().await?;
    log::info!("index_filter: {:?}, indices: {:#?}", index_filter, indices);
    let now = Utc::now();
    let outdated_indices = indices
        .iter()
        .filter(|i| {
            now.signed_duration_since(i.creation_date).num_days()
                > keep_days as i64
        })
        .map(|i| i.index.to_string())
        .collect::<Vec<_>>();

    log::info!("indices(> {} days): {:#?}", keep_days, outdated_indices);
    Ok(outdated_indices)
}

/// Take an elasticsearch snapshot, use the index name as snapshot name
///
/// If you send GET request to query the status of the snapshot through the
/// `_snapshot` api, i.e `GET /_snapshot/elasticsearch-snapshot-log-repo/test`,
/// the response will be like:
///
/// ```json
/// {
///   "snapshots" : [ {
///     "uuid" : "37CRhvKtQdKward_wVGaVg",
///     "version_id" : 7010199,
///     "version" : "7.1.1",
///     "indices" : [ "test-index" ],
///     "include_global_state" : false,
///     "state" : "SUCCESS",
///     "start_time_in_millis" : 1622431908495,
///     "end_time" : "2021-05-31T03:31:49.794Z",
///     "end_time_in_millis" : 1622431909794,
///     "duration_in_millis" : 1299,
///     "failures" : [ ],
///     "shards" : {
///       "total" : 1,
///       "failed" : 0,
///       "successful" : 1
///     }
///   } ]
/// }
/// ```
async fn take_snapshot(
    client: &Elasticsearch, repository: &str, index: &str,
) -> anyhow::Result<()> {
    log::info!("taking snapshot for {}, repository: {}", index, repository);
    let response = client
        .snapshot()
        .create(SnapshotCreateParts::RepositorySnapshot(repository, index))
        .body(json!({
          "indices": index,
          "ignore_unavailable": true,
          "include_global_state": false,
          "metadata": {
            "taken_by": "elasticsearch-index-cleaner",
            "taken_because": "scheduled backup"
          }
        }))
        .send()
        .await?;

    let body = response.text().await?;
    log::info!("take snapshot response: {:?}", body);

    Ok(())
}

/// Take an elasticsearch snapshot, use the index name as snapshot name
/// and check the snapshot status. If the snapshot is successfully taken, return
/// immediately. Otherwise, it will sleep and wait snapshot to be successful.
pub async fn take_snapshot_and_check(
    client: &Elasticsearch, repository: &str, index: &str,
) -> anyhow::Result<()> {
    loop {
        let snapshot_running = is_snapshot_running(client).await?;
        // if any snapshot is running, we'll wait it to be finished.
        if snapshot_running {
            // TODO: we should log the running snapshot.
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }
        break;
    }

    take_snapshot(client, repository, index).await?;

    loop {
        if !is_snapshot_success(client, repository, index).await? {
            log::info!("snapshot {} is not ready, sleep 10s...", index);
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }
        break;
    }
    Ok(())
}

/// Check snapshot status, true if snapshot has been successful taken, otherwise
/// return false
///
/// The response from elasticsearch `_snapshot` api looks like this:
///
/// ```json
/// {
///   "snapshots" : [ {
///     "snapshot" : "logstash-2021.05.11",
///     "uuid" : "BAtz3c9lTlud4Qn__HeqWA",
///     "version_id" : 7010199,
///     "version" : "7.1.1",
///     "indices" : [ "logstash-2021.05.11" ],
///     "include_global_state" : false,
///     "state" : "SUCCESS",
///     "start_time" : "2021-05-23T02:17:58.280Z",
///     "start_time_in_millis" : 1621736278280,
///     "end_time" : "2021-05-23T02:37:51.199Z",
///     "end_time_in_millis" : 1621737471199,
///     "duration_in_millis" : 1192919,
///     "failures" : [ ],
///     "shards" : {
///       "total" : 1,
///       "failed" : 0,
///       "successful" : 1
///     }
///   } ]
/// }
/// ```
pub async fn is_snapshot_success(
    client: &Elasticsearch, repository: &str, snapshot: &str,
) -> anyhow::Result<bool> {
    let response = client
        .snapshot()
        .status(SnapshotStatusParts::RepositorySnapshot(
            repository,
            &[snapshot],
        ))
        .send()
        .await?;

    log::debug!("snapshot status check response: {:?}", response);
    let snapshots = response.json::<Snapshots>().await?;
    log::info!("snapshots status: {:?}", snapshots);

    let count = snapshots
        .snapshots
        .iter()
        .filter(|s| s.snapshot == snapshot && s.state == "SUCCESS")
        .count();

    if count > 0 {
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Delete index from elasticsearch
///
/// This will only send DELETE request to elasticsearch endpoint, and discards
/// the response.
async fn delete_index(
    client: &Elasticsearch, index: &str,
) -> anyhow::Result<()> {
    let response = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[index]))
        .send()
        .await?;
    let body = response.text().await?;
    log::info!("delete index: {}, response: {:?}", index, body);
    Ok(())
}

/// Check if snapshot is running under specified repository, return true if
/// snapshot is running, otherwise return false.
async fn is_snapshot_running(client: &Elasticsearch) -> anyhow::Result<bool> {
    log::info!("checking all snapshot status ...");
    let response =
        client.snapshot().status(SnapshotStatusParts::None).send().await?;

    log::info!("snapshot status for response: {:?}", response);
    let snapshots = response.json::<Snapshots>().await?;

    Ok(!snapshots.snapshots.is_empty())
}
