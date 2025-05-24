# drylab/tools/llm.py
import hashlib, json, os, asyncio
from drylab.llms.gpt import OpenAIGPT
from ..types import Blob           # reuse your NewType
from ..ledger import Ledger
from ..types import EventHeader, EventRow

_CHAT_SCHEMA = "OPENAI_CHAT@1"

class OpenAIGPT:
    def __init__(self, ledger: Ledger, model="gpt-4o-mini"):
        self.ledger = ledger
        self.client = OpenAIGPT()
        self.model = model

    async def chat(self, messages: list[dict]) -> str:
        payload = json.dumps({"m": messages, "model": self.model}).encode()
        sha = hashlib.sha256(payload).hexdigest()

        # ➊ check if we answered this prompt before
        try:
            cached = self.ledger.cat(sha)
            return cached.decode()
        except KeyError:
            pass

        # ➋ otherwise call the API
        resp = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
        )
        answer = resp.choices[0].message.content

        # ➌ store answer as its own Artifact so provenance tracks the LLM, too
        header = EventHeader(id=sha, schema_id=_CHAT_SCHEMA)
        self.ledger.publish(EventRow(header=header,
                                     blob=Blob(answer.encode()),
                                     run_id="llm-cache", seq=0))
        return answer
