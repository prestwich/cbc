use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::net::SocketAddr;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

fn telemetry(endpoint: Option<&str>) {
    let otel = endpoint.map(|base| {
        let resource = Resource::builder().with_service_name("cbc").build();
        let spans = SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{base}/v1/traces"))
            .build()
            .unwrap();
        let metrics = MetricExporter::builder()
            .with_http()
            .with_endpoint(format!("{base}/v1/metrics"))
            .build()
            .unwrap();
        let meters = SdkMeterProvider::builder()
            .with_periodic_exporter(metrics)
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_meter_provider(meters);
        let tracers = SdkTracerProvider::builder()
            .with_batch_exporter(spans)
            .with_resource(resource)
            .build();
        tracing_opentelemetry::layer().with_tracer(tracers.tracer("cbc"))
    });
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(otel)
        .init();
}

#[tokio::main]
async fn main() {
    let config = cbc::Config::from_env().unwrap_or_else(|e| exit(e.to_string()));
    if let Err(e) = config.validate() {
        exit(e);
    }
    telemetry(config.otlp_endpoint.as_deref());
    let listener = tokio::net::TcpListener::bind(&config.bind).await.unwrap();
    let service = cbc::app(config).into_make_service_with_connect_info::<SocketAddr>();
    axum::serve(listener, service).await.unwrap();
}

fn exit(message: String) -> ! {
    eprintln!("config error: {message}");
    std::process::exit(2)
}
