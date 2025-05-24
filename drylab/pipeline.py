# drylab/pipeline.py
import asyncio
import logging
from typing import Type, Optional
from .ledger import Ledger
from .reactor import Reactor

logging.getLogger("drylab.pipeline").setLevel(logging.DEBUG)

class Pipeline:
    def __init__(
        self,
        db_path: str | None = "pipeline.db",
        *,
        idle_timeout: Optional[float] = None,
        logger: logging.Logger | None = None,
    ):
        self.ledger = Ledger(db_path)
        self._tasks: list[asyncio.Task] = []
        self._idle_timeout = idle_timeout
        self._activity = asyncio.Event()
        # -----------------------------------------------------------------
        self.log = logger or logging.getLogger("drylab.pipeline")
        # If user hasn’t configured logging, default to something reasonable
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
            )
        self.log.debug("Pipeline initialised (idle_timeout=%s)", idle_timeout)

    # ---------------------------------------------------------------------
    def add(self, reactor_cls: Type[Reactor], *, run_id: str, **kwargs):
        rx = reactor_cls(self.ledger, activity_event=self._activity, **kwargs)
        task = asyncio.create_task(rx.run(run_id), name=reactor_cls.__name__)
        self._tasks.append(task)
        self.log.debug("Added reactor %s for run_id=%s", reactor_cls.__name__, run_id)

    # ---------------------------------------------------------------------
    async def _watchdog(self):
        if self._idle_timeout is None:
            return
        while True:
            try:
                await asyncio.wait_for(
                    self._activity.wait(), timeout=self._idle_timeout
                )
                self._activity.clear()
            except asyncio.TimeoutError:
                self.log.info(
                    "No new events for %.1f s — shutting down pipeline.",
                    self._idle_timeout,
                )
                for t in self._tasks:
                    self.log.debug("Cancelling task %s", t.get_name())
                    t.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                self.log.info("Pipeline stopped gracefully.")
                return

    # ---------------------------------------------------------------------
    async def run_forever(self):
        results = await asyncio.gather(
            self._watchdog(), *self._tasks, return_exceptions=True
        )
        # Log unexpected errors
        for r in results:
            if isinstance(r, Exception) and not isinstance(
                r, asyncio.CancelledError
            ):
                self.log.error("Task failed: %s", r, exc_info=r)
