# drylab/tools/llm.py
import hashlib, json, os, asyncio
from google import genai
from ...types import Blob           # reuse your NewType
from ...ledger import Ledger
from ...types import EventHeader, EventRow

_CHAT_SCHEMA = "GEMINI_CHAT@1"

class GoogleGemini:
    def __init__(self, ledger: Ledger, model="gemini-2.5-flash", api_key=None):
        self.ledger = ledger
        self.client = genai.Client(api_key=api_key)
        self.model = model

    async def chat(self, content: list[dict]) -> str:
        payload = json.dumps({"content": content, "model": self.model}).encode()
        sha = hashlib.sha256(payload).hexdigest()

        # ➊ check if we answered this prompt before
        try:
            cached = self.ledger.cat(sha)
            return cached.decode()
        except KeyError:
            pass

        # ➋ otherwise call the API
        resp = await self.client.models.generate_content(
            model=self.model, 
            contents=self.content
        )
        answer = resp.text

        # ➌ store answer as its own Artifact so provenance tracks the LLM, too
        header = EventHeader(id=sha, schema_id=_CHAT_SCHEMA)
        self.ledger.publish(EventRow(header=header,
                                     blob=Blob(answer.encode()),
                                     run_id="llm-cache", seq=0))
        return answer
