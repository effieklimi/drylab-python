# drylab/tools/llm.py
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

from google import genai

from ..ledger import Ledger
from ..types import Blob, EventHeader, EventRow, SchemaId, Sha256
from .env import GEMINI_API_KEY

# -----------------------------------------------------------------------------#
# 1 ▸ Shared executor & semaphore (one per interpreter)                        #
# -----------------------------------------------------------------------------#
_DEFAULT_THREADS = max(32, (os.cpu_count() or 1) + 4)
_LLM_POOL = ThreadPoolExecutor(
    max_workers=int(os.getenv("DRYLAB_THREADS", _DEFAULT_THREADS)),
    thread_name_prefix="llm-worker",
)
_LLM_SEM = asyncio.Semaphore(
    int(os.getenv("DRYLAB_MAX_LLM_CONCURRENCY", 32))
)

# -----------------------------------------------------------------------------#
# 2 ▸ Schema identifier for caching                                            #
# -----------------------------------------------------------------------------#
CHAT_SCHEMA = SchemaId("GEMINI_CHAT@1")


# -----------------------------------------------------------------------------#
# 3 ▸ Helper class                                                             #
# -----------------------------------------------------------------------------#
class GoogleGemini:
    """
    A provenance-aware Gemini wrapper that:

    • Off-loads blocking SDK calls to a shared ThreadPoolExecutor.
    • Respects a global concurrency semaphore.
    • Caches prompt→response pairs in the Ledger so identical prompts
      are not sent twice (idempotency / cost savings).
    """

    def __init__(
        self,
        ledger: Ledger,
        model: str = "gemini-2.5-flash-preview-04-17",
        api_key: str = GEMINI_API_KEY,
    ):
        self.ledger = ledger
        self.model = model
        self.client = genai.Client(api_key=api_key)

    # ------------------------------------------------------------------ #
    # internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _prompt_hash(self, contents: List[str]) -> Sha256:
        payload = json.dumps({"m": contents, "model": self.model}).encode()
        return Sha256(hashlib.sha256(payload).hexdigest())

    def _blocking_generate(self, contents: List[str]):
        """Sync wrapper around the Gemini SDK call."""
        return self.client.models.generate_content(
            contents=contents,
            model=self.model,
        )

    # ------------------------------------------------------------------ #
    # public async chat()                                                #
    # ------------------------------------------------------------------ #
    async def chat(self, messages: list[dict]) -> str:
        # 1 ▸ Flatten messages to List[str]
        contents: list[str] = [
            f"{m.get('role', '').upper()}: {m['content']}" for m in messages
        ]

        # 2 ▸ Check cache
        sha = self._prompt_hash(contents)
        try:
            cached = self.ledger.cat(sha)
            return cached.decode("utf-8")
        except KeyError:
            pass  # miss → proceed to call LLM

        # 3 ▸ Bound concurrency + off-load to thread-pool
        async with _LLM_SEM:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                _LLM_POOL, self._blocking_generate, contents
            )

        parts = resp.candidates[0].content.parts
        answer = "".join(p.text for p in parts)

        # 4 ▸ Persist response for provenance & caching next time
        header = EventHeader(id=sha, schema_id=CHAT_SCHEMA)
        self.ledger.publish(
            EventRow(
                header=header,
                blob = Blob(answer.encode("utf-8")),
                run_id="llm-cache",
                seq=0,
            )
        )

        return answer
