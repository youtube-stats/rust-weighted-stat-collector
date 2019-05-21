extern crate postgres;
extern crate rand;
extern crate reqwest;
extern crate serde_json;
extern crate serde;

use rand::distributions::Distribution;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct PageInfoType {
    #[allow(dead_code)]
    totalResults: u8,

    #[allow(dead_code)]
    resultsPerPage: u8
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct StatisticsType {
    viewCount: String,

    #[allow(dead_code)]
    commentCount: String,

    subscriberCount: String,

    #[allow(dead_code)]
    hiddenSubscriberCount: bool,

    videoCount: String
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct  ItemType {
    #[allow(dead_code)]
    kind: String,

    #[allow(dead_code)]
    etag: String,

    id: String,
    statistics: StatisticsType
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct YoutubeResponseType {
    #[allow(dead_code)]
    kind: String,

    #[allow(dead_code)]
    etag: String,

    #[allow(dead_code)]
    nextPageToken: String,

    #[allow(dead_code)]
    pageInfo: PageInfoType,

    items: Vec<ItemType>
}

struct Channel {
    id: i32,
    serial: String
}

fn main() {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    let conn: postgres::Connection =
        postgres::Connection::connect(params, tls).unwrap();

    let key: String = std::env::var("YOUTUBE_KEY").unwrap();
    let mut offset: u32 = 0;


    let query: &str = "select
       a.channel_id,
       c.serial,
       (a.lasty - b.subs) diff
    from
         (SELECT
           channel_id,
           last(subs::bigint, time) lasty
        FROM youtube.stats.metrics
    where now() - interval '10 minutes' > time
        GROUP BY channel_id
    order by lasty desc) a
    inner join
             (SELECT
           channel_id,
           last(subs::bigint, time) subs
      FROM youtube.stats.metrics
      GROUP BY channel_id) b
    on a.channel_id = b.channel_id
    inner join youtube.stats.channels c
    on a.channel_id = c.id";

    loop {
        let rows: postgres::rows::Rows = conn.query(query, &[]).unwrap();

        let mut hash: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();

        let mut channels: Vec<Channel> = Vec::new();
        let mut weights: Vec<u64> = Vec::new();

        for row in &rows {
            let channel_id: i32 = row.get(0);
            let channel_serial: String = row.get(1);
            let diff: i64 = row.get(2);

            hash.insert(channel_serial.clone(), channel_id);

            channels.push(Channel {
                id: channel_id,
                serial: channel_serial
            });

            let value: u64 = i64::abs(diff) as u64;
            weights.push(value);
        }

        println!("Retrieved {} channels", weights.len());

        if weights.iter().cloned().fold(0, u64::min) == 0 {
            let range: std::ops::Range<usize> = 0..(weights.len());

            println!("Min is 0 - Adding 1 to all members");

            for i in range {
                weights[i] += 1;
            }
        }


        let dist =
            rand::distributions::WeightedIndex::new(&weights).unwrap();
        let mut rng: rand::prelude::ThreadRng = rand::prelude::thread_rng();

        for _i in 1..10000 {
            let mut vec_id: Vec<String> = Vec::new();
            for _i in 0..50 {
                let random: usize = dist.sample(&mut rng);
                let value: &String = &channels[random].serial;
                vec_id.push(value.clone());
            }

            let ids: String = vec_id.join(",");
            let url: String =
                format!("https://www.googleapis.com/youtube/v3/channels?part=statistics&key={}&id={}",
                        key, ids);

            let mut resp: reqwest::Response = match reqwest::get(url.as_str()) {
                Ok(resp) => resp,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            let body: String = match resp.text() {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            let response: YoutubeResponseType = match serde_json::from_str(body.as_str()) {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            for item in response.items {
                let channel_id: String = match hash.get(item.id.as_str()) {
                    Some(text) => text.to_string(),
                    None => {
                        eprintln!("Found no value for key {}", item.id);
                        continue
                    }
                };

                println!("{} {} {} {} {}",
                         item.id,
                         channel_id,
                         item.statistics.subscriberCount,
                         item.statistics.viewCount,
                         item.statistics.videoCount);

                let query: String =
                    format!("INSERT INTO youtube.stats.metrics (channel_id, subs, views, videos) VALUES ({}, {}, {}, {})",
                            channel_id,
                            item.statistics.subscriberCount,
                            item.statistics.viewCount,
                            item.statistics.videoCount);

                let n: u64 = match conn.execute(query.as_str(), &[]) {
                    Ok(size) => size,
                    Err(e) => {
                        eprintln!("{}", e.to_string());
                        continue
                    }
                };

                if n != 1 {
                    eprintln!("Row did not insert correctly");
                }
            }
        }
    }
}
