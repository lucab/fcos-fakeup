#[macro_use]
extern crate log;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate prometheus;

mod metadata;
mod scraper;

use actix::prelude::*;
use actix_web::{http::Method, middleware::Logger, server, App};
use actix_web::{HttpRequest, HttpResponse};
use failure::{Error, Fallible, format_err};
use futures::future;
use futures::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use structopt::StructOpt;

fn main() -> Fallible<()> {
    env_logger::Builder::from_default_env().try_init()?;

    let opts = CliOptions::from_args();
    trace!("starting with config: {:#?}", opts);

    let sys = actix::System::new("fakeup");
    let streams = btreeset!(
        "bodhi-updates".to_string(),
        "testing".to_string(),
        "testing-devel".to_string(),
    );
    let refresh_pause = std::time::Duration::from_secs(30);
    let scraper_addr = scraper::Scraper::new(streams, refresh_pause)?.start();
    let app_state = AppState { scraper_addr };

    server::new(move || {
        App::with_state(app_state.clone())
            .middleware(Logger::default())
            .route("/v1/graph", Method::GET, serve_graph)
    })
    .bind((IpAddr::from(Ipv4Addr::UNSPECIFIED), opts.port))?
    .start();

    sys.run();
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct AppState {
    pub(crate) scraper_addr: Addr<scraper::Scraper>,
}

pub(crate) fn serve_graph(
    req: HttpRequest<AppState>,
) -> Box<Future<Item = HttpResponse, Error = Error>> {
    // Get client OS version.
    let current_os = req.query().get("current_os").cloned().unwrap_or_default();
    let os_checksum = req.query().get("os_checksum").cloned().unwrap_or_default();
    let mut os = current_os;
    if os.is_empty() {
        os = os_checksum;
        if os.is_empty() {
            return Box::new(future::ok(HttpResponse::BadRequest().finish()));
        }
    }
    trace!("client OS checksum: {}", os);

    // Get client OS stream.
    let stream = req.query().get("stream").cloned().unwrap_or_default();
    if stream.is_empty() {
        return Box::new(future::ok(HttpResponse::BadRequest().finish()));
    }
    trace!("client stream: {}", os);

    // Synthesize source node.
    let current = CincinnatiPayload {
        version: "client-os-version".to_string(),
        payload: os,
        metadata: hashmap! {
            "org.fedoraproject.coreos.scheme".to_string() => "checksum".to_string(),
            "org.fedoraproject.coreos.releases.age_index".to_string() => "0".to_string(),
        },
    };

    let cached_latest = req
        .state()
        .scraper_addr
        .send(scraper::GetLatest::new("x86_64".to_string(), stream))
        .flatten();

    // Assemble graph and return it as JSON.
    let resp = cached_latest
        .and_then(|latest| {
            let graph = Graph {
                nodes: vec![current, latest],
                edges: vec![(0, 1)],
            };
            Ok(graph)
        })
        .from_err()
        .and_then(|graph| serde_json::to_string_pretty(&graph).map_err(|e| format_err!("{}", e)))
        .map(|json| {
            HttpResponse::Ok()
                .content_type("application/json")
                .body(json)
        });

    Box::new(resp)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Graph {
    pub(crate) nodes: Vec<CincinnatiPayload>,
    pub(crate) edges: Vec<(u64, u64)>,
}

#[derive(Debug, StructOpt)]
pub(crate) struct CliOptions {
    /// Port to which the server will bind.
    #[structopt(short = "p", long = "port", default_value = "9876")]
    port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CincinnatiPayload {
    pub(crate) version: String,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) payload: String,
}
