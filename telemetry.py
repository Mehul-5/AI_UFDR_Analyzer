from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

def configure_telemetry(app=None, is_worker=False):
    # Initialize the Tracer Provider
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
    
    # Configure an exporter (Using Console for local debugging; replace with OTLP in production)
    exporter = ConsoleSpanExporter()
    
    # Use BatchSpanProcessor to avoid blocking the main thread
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    
    # Instrument Postgres globally
    AsyncPGInstrumentor().instrument()

    # Instrument FastAPI if passed
    if app:
        FastAPIInstrumentor.instrument_app(app)
        
    # Instrument Celery if this is a worker node
    if is_worker:
        CeleryInstrumentor().instrument()