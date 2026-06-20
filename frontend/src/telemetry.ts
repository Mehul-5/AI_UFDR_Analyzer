import { WebTracerProvider } from '@opentelemetry/sdk-trace-web';
import { DocumentLoadInstrumentation } from '@opentelemetry/instrumentation-document-load';
import { FetchInstrumentation } from '@opentelemetry/instrumentation-fetch';
import { XMLHttpRequestInstrumentation } from '@opentelemetry/instrumentation-xml-http-request';
import { ZoneContextManager } from '@opentelemetry/context-zone';
import { registerInstrumentations } from '@opentelemetry/instrumentation';

export const initializeTelemetry = () => {
  // BYPASS THE ERROR: Pass the span processor directly into the constructor
  const provider = new WebTracerProvider({
    spanProcessors: [
      // new SimpleSpanProcessor(new ConsoleSpanExporter())
    ]
  });

  // Context manager is required in browsers to keep track of async operations
  provider.register({
    contextManager: new ZoneContextManager(),
  });

  // Automatically instrument browser page loads and network requests
  registerInstrumentations({
    instrumentations: [
      new DocumentLoadInstrumentation(),
      new XMLHttpRequestInstrumentation({
        propagateTraceHeaderCorsUrls: [
          /http:\/\/.+:8000.*/, // 🚀 FIX: Allow trace headers to be sent to any host IP
          /^\/api\/.*/ 
        ],
      }),
      new FetchInstrumentation({
        propagateTraceHeaderCorsUrls: [
          /http:\/\/.+:8000.*/, // 🚀 FIX: Allow trace headers to be sent to any host IP
          /^\/api\/.*/ 
        ],
      }),
    ],
  });
  
  console.log(" OpenTelemetry Web Instrumentation Initialized");
};