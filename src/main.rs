use std::{sync::Arc};

use actix::Actor;
use actix::Addr;
use actix::Handler;
use actix::Message;
use actix::SyncArbiter;
use actix::SyncContext;
use actix::System;
use actix_web::{App, AsyncResponder, FutureResponse, HttpRequest, HttpResponse, server};
use actix_web::Error as AWError;
use actix_web::error::ErrorInternalServerError;
use actix_web::http::Method;
use actix_web::Json;
use futures::future;
use futures::Future;
use rusoto_core::Region;
use rusoto_kinesis::{Kinesis, KinesisClient, PutRecordInput};
use serde_json;
use zipkin_types::Span;

pub struct KinesisExecutor(KinesisClient);

pub struct SendRecord {
    pub data: Span,
}

impl Message for SendRecord {
    type Result = Result<(), AWError>;
}

impl Handler<SendRecord> for KinesisExecutor {
    type Result = Result<(), AWError>;

    fn handle(&mut self, input: SendRecord, _: &mut Self::Context) -> Self::Result {
        self.0.put_record(PutRecordInput { data: serde_json::to_vec(&input.data).unwrap(), partition_key: input.data.id().to_string(), stream_name: "test".to_string(), ..Default::default() })
            .sync()
            .map(|_| ())
            .map_err(|err| ErrorInternalServerError(err))
    }
}

impl Actor for KinesisExecutor {
    type Context = SyncContext<Self>;
}

pub struct AppState {
    pub executor: Arc<Addr<KinesisExecutor>>,
}

impl AppState {
    pub fn new(executor: Addr<KinesisExecutor>) -> Self {
        AppState { executor: Arc::new(executor) }
    }
}

fn put_record((target, req): (Json<Span>, HttpRequest<AppState>)) -> impl Future<Item=HttpResponse, Error=AWError> {
    let addr: Arc<Addr<KinesisExecutor>> = req.state().executor.clone();
    let actor = addr.send(SendRecord { data: target.0 });

    actor.map_err(|error| actix_web::error::ErrorInternalServerError(format!("{:?}", error)))
        .and_then(|r| match r {
            Ok(_) => Ok(HttpResponse::Ok().finish()),
            Err(_) => {
                Ok(HttpResponse::InternalServerError().finish())
            }
        }).responder()
}

pub fn preflight_insert_record(_: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    future::ok(HttpResponse::Ok().finish()).responder()
}

fn main() {
    let sys = System::new("kinesis_proxy");
    let addr = SyncArbiter::start(3, move || KinesisExecutor(KinesisClient::new(Region::ApNortheast1)));
    server::new(
        move || App::with_state(AppState::new(addr.clone()))
            .resource("/", |r| {
                r.method(Method::OPTIONS)
                    .with_async(preflight_insert_record);
                r.method(Method::POST).with_async(put_record)
            })
    )
        .bind("127.0.0.1:8080").unwrap()
        .run();
}