use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{self, Deserialize, Deserializer};

// The signature of a deserialize_with function must follow the pattern:
//
//    fn deserialize<'de, D>(D) -> Result<T, D::Error>
//    where
//        D: Deserializer<'de>
//
// although it may also be generic over the output types T.
pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    // NOTE:
    // elasticsearch cat api return creation_date as String not integer, we need to convert to
    // i64, then to a DateTime
    let t = s.parse::<i64>().map_err(serde::de::Error::custom)?;
    Ok(DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp(t / 1000, (t as u32) % 1000),
        Utc,
    ))
}
