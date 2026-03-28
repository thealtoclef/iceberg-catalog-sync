"""OpenTelemetry metrics and tracing setup."""

from __future__ import annotations

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from iceberg_catalog_sync.config import MetricsConfig, TracingConfig


def _get_otlp_span_exporter(endpoint: str):  # noqa: ANN202
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )

    return OTLPSpanExporter(endpoint=endpoint)


def _get_otlp_metric_exporter(endpoint: str):  # noqa: ANN202
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )

    return OTLPMetricExporter(endpoint=endpoint)


def setup_tracing(config: TracingConfig) -> trace.Tracer:
    """Return a real or no-op tracer based on config."""
    if not config.enabled:
        return trace.get_tracer("iceberg_catalog_sync")

    resource = Resource.create({"service.name": config.service_name})
    if config.exporter == "otlp":
        exporter = _get_otlp_span_exporter(config.otlp_endpoint)
    else:
        exporter = ConsoleSpanExporter()

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("iceberg_catalog_sync")


def setup_metrics(config: MetricsConfig) -> metrics.Meter:
    """Return a real or no-op meter based on config."""
    if not config.enabled:
        return metrics.get_meter("iceberg_catalog_sync")

    if config.exporter == "otlp":
        exporter = _get_otlp_metric_exporter(config.otlp_endpoint)
    else:
        exporter = ConsoleMetricExporter()

    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    return metrics.get_meter("iceberg_catalog_sync")
