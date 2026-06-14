import cohere
from abc import ABC, abstractmethod
import logging
from config import settings

logger = logging.getLogger(__name__)

# 1. The Abstract Interface
class LLMGatewayInterface(ABC):
    @abstractmethod
    async def synthesize(self, prompt: str, system_message: str, documents: list) -> dict:
        pass

# 2. The Cohere Implementation
class CohereStrategy(LLMGatewayInterface):
    def __init__(self):
        self.client = cohere.AsyncClient(settings.COHERE_API_KEY)

    async def synthesize(self, prompt: str, system_message: str, documents: list) -> dict:
        response = await self.client.chat(
            message=prompt,
            model="command-r-08-2024",
            preamble=system_message,
            documents=documents if documents else None
        )
        
        citations = [{"text": c.text, "docs": c.document_ids} for c in response.citations] if response.citations else []
        return {"answer": response.text, "citations": citations}

# 3. The Circuit Breaker Proxy
class CircuitBreakerLLM:
    def __init__(self, strategy: LLMGatewayInterface):
        self.strategy = strategy
        self.failure_count = 0
        self.threshold = 3

    async def generate_response(self, prompt: str, system_message: str, documents: list) -> dict:
        if self.failure_count >= self.threshold:
            logger.warning("CIRCUIT BREAKER OPEN: Returning raw un-synthesized context to user.")
            return {
                "answer": "AI Synthesis is currently degraded. Returning raw forensic evidence.",
                "citations": [],
                "raw_context": documents
            }

        try:
            result = await self.strategy.synthesize(prompt, system_message, documents)
            self.failure_count = 0  # Reset on success
            return result
        except Exception as e:
            # Catch Rate Limits / 429s
            self.failure_count += 1
            logger.error(f"LLM API Failure ({self.failure_count}/{self.threshold}): {str(e)}")
            return await self.generate_response(prompt, system_message, documents) # Retry or fail open