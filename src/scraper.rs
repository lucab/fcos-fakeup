use crate::metadata;
use crate::CincinnatiPayload;
use actix::prelude::*;
use failure::{Error, Fallible};
use futures::future;
use futures::prelude::*;
use prometheus::{IntCounter, IntGauge};
use reqwest::Method;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

lazy_static::lazy_static! {
    static ref LAST_REFRESH: IntGauge = register_int_gauge!(opts!(
        "fakeup_scraper_last_refresh_timestamp",
        "UTC timestamp of last refresh"
    )).unwrap();
    static ref UPSTREAM_SCRAPES: IntCounter = register_int_counter!(opts!(
        "fakeup_scraper_upstream_scrapes_total",
        "Total number of upstream scrapes"
    ))
    .unwrap();
}

/// Release scraper.
#[derive(Clone, Debug)]
pub struct Scraper {
    hclient: reqwest::r#async::Client,
    latest: HashMap<String, metadata::Release>,
    refresh_pause: Duration,
    streams: BTreeSet<String>,
}

impl Scraper {
    pub fn new(streams: BTreeSet<String>, refresh_pause: Duration) -> Fallible<Self> {
        let scraper = Self {
            hclient: reqwest::r#async::ClientBuilder::new().build()?,
            latest: HashMap::new(),
            refresh_pause,
            streams,
        };
        Ok(scraper)
    }

    /// Return a request builder with base URL and parameters set.
    fn new_request(
        &self,
        method: reqwest::Method,
        stream: String,
    ) -> Fallible<reqwest::r#async::RequestBuilder> {
        let vars = hashmap!("stream".to_string() => stream);
        let full = envsubst::substitute(metadata::RELEASES_JSON, &vars)?;
        let url = reqwest::Url::parse(&full)?;
        let builder = self.hclient.request(method, url);
        Ok(builder)
    }

    /// Fetch last release from release-index.
    fn fetch_releases(
        &self,
        stream: &str,
    ) -> impl Future<Item = (String, Option<metadata::Release>), Error = Error> {
        let out_stream = stream.to_string();
        let req = self.new_request(Method::GET, stream.to_string());
        future::result(req)
            .and_then(|req| req.send().from_err())
            .and_then(|resp| resp.error_for_status().map_err(Error::from))
            .and_then(|mut resp| resp.json::<metadata::ReleasesJSON>().from_err())
            .map(|mut json| (out_stream, json.releases.pop()))
    }

    /// Refresh cache.
    fn refresh_cache(
        &self,
    ) -> impl Future<Item = HashMap<String, metadata::Release>, Error = Error> {
        let mut latest: Vec<_> = Vec::new();
        for stream in &self.streams {
            let fut = self.fetch_releases(stream);
            latest.push(fut);
        }

        let cache = future::join_all(latest).map(|vec| {
            let mut streams_latest = HashMap::new();
            for (stream, latest) in vec {
                if let Some(rel) = latest {
                    streams_latest.insert(stream, rel);
                }
            }
            streams_latest
        });
        cache
    }
}

impl Actor for Scraper {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Kick-start the state machine.
        Self::tick_now(ctx);
    }
}

pub(crate) struct RefreshTick {}

impl Message for RefreshTick {
    type Result = Result<(), Error>;
}

impl Handler<RefreshTick> for Scraper {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _msg: RefreshTick, ctx: &mut Self::Context) -> Self::Result {
        UPSTREAM_SCRAPES.inc();

        let updates = self.refresh_cache();

        let update_graph = actix::fut::wrap_future::<_, Self>(updates)
            .map_err(|err, _actor, _ctx| log::error!("{}", err))
            .map(|cache, actor, _ctx| {
                actor.latest = cache;
                let refresh_timestamp = chrono::Utc::now();
                LAST_REFRESH.set(refresh_timestamp.timestamp());
            })
            .then(|_r, actor, ctx| {
                Self::tick_later(ctx, actor.refresh_pause);
                actix::fut::ok(())
            });

        ctx.wait(update_graph);

        Box::new(actix::fut::ok(()))
    }
}

pub(crate) struct GetLatest {
    pub(crate) basearch: String,
    pub(crate) stream: String,
}

impl GetLatest {
    pub fn new(basearch: String, stream: String) -> Self {
        Self { basearch, stream }
    }
}

impl Message for GetLatest {
    type Result = Result<CincinnatiPayload, Error>;
}

impl Handler<GetLatest> for Scraper {
    type Result = ResponseActFuture<Self, CincinnatiPayload, Error>;
    fn handle(&mut self, msg: GetLatest, _ctx: &mut Self::Context) -> Self::Result {
        let release = match self.latest.get(&msg.stream) {
            None => return Box::new(actix::fut::err(failure::format_err!("stream unavailable"))),
            Some(latest) => latest,
        };

        let checksum = {
            let mut matching = String::new();
            for commit in &release.commits {
                if commit.architecture == msg.basearch {
                    matching = commit.checksum.to_string();
                }
            }
            if matching.is_empty() {
                return Box::new(actix::fut::err(failure::format_err!(
                    "basearch unavailable"
                )));
            }
            matching
        };

        let node = CincinnatiPayload {
            version: release.version.clone(),
            payload: checksum,
            metadata: hashmap! {
                "org.fedoraproject.coreos.scheme".to_string() => "checksum".to_string(),
                "org.fedoraproject.coreos.releases.age_index".to_string() => "1".to_string(),
            },
        };

        return Box::new(actix::fut::ok(node));
    }
}

impl Scraper {
    /// Schedule an immediate refresh the state machine.
    pub fn tick_now(ctx: &mut Context<Self>) {
        ctx.notify(RefreshTick {})
    }

    /// Schedule a delayed refresh of the state machine.
    pub fn tick_later(ctx: &mut Context<Self>, after: std::time::Duration) -> actix::SpawnHandle {
        ctx.notify_later(RefreshTick {}, after)
    }
}
