# python
# File: `qcrawl/runner/export.py`

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from qcrawl import exporters as _exporter
from qcrawl import signals
from qcrawl.storage import Storage

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.spider import Spider


class ExportError(RuntimeError):
    """Raised when an export transaction cannot be committed.

    The first failure encountered while serializing items or writing the
    destination is retained as the cause. A failed transaction never replaces a
    previously committed file.
    """


@dataclass
class ExportTransaction:
    """Tracks the outcome of one spider's export.

    Signal dispatch swallows handler exceptions by default, so export handlers
    record their first error here instead of raising. The runner checks the
    transaction after the crawl and re-raises, turning a failed export into a
    non-zero process exit.
    """

    error: BaseException | None = None
    item_count: int = 0
    committed: bool = False
    _chunks: list[bytes] = field(default_factory=list)

    def fail(self, exc: BaseException, context: str) -> None:
        """Record the first export failure; later failures are logged only."""
        if self.error is None:
            self.error = exc
            logger.error("Export failed during %s: %s", context, exc)
        else:
            logger.error("Additional export failure during %s: %s", context, exc, exc_info=exc)

    @property
    def failed(self) -> bool:
        """True once any phase of the transaction has failed (no commit allowed)."""
        return self.error is not None

    def raise_if_failed(self) -> None:
        """Re-raise the recorded export failure, if any."""
        if self.error is not None:
            raise ExportError(
                f"Export failed after {self.item_count} item(s); "
                "the previous output file (if any) was kept"
            ) from self.error


def build_exporter(
    format: str | None, mode: str = "buffered", buffer_size: int = 500
) -> _exporter.Exporter:
    """Return an exporter instance for the given format/mode."""
    fmt = (format or "").lower()
    m = (mode or "").lower()

    if fmt == "ndjson":
        return _exporter.JsonLinesExporter()
    if fmt == "json":
        if m == "buffered":
            return _exporter.JsonBufferedExporter(buffer_size)
        return _exporter.JsonLinesExporter()
    if fmt == "csv":
        return _exporter.CsvExporter()
    if fmt == "xml":
        return _exporter.XmlExporter()

    raise ValueError(f"Unknown export format: {format!r}")


def _as_bytes(data: bytes | str | None) -> bytes:
    if data is None:
        return b""
    if isinstance(data, str):
        return data.encode("utf-8")
    return bytes(data)


def _commit_file_sync(destination: Path, payload: bytearray) -> None:
    """Atomically replace `destination` with `payload`.

    Bytes are staged in a temp file in the same directory and os.replace()d into
    place only after a full flush+fsync. Any failure removes the temp file and
    leaves the previous destination untouched.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, destination)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise


def register_export_handlers(
    dispatcher: signals.SignalDispatcher,
    exporter: _exporter.Exporter,
    crawler: object,
    *,
    storage: Storage | None,
    file_path: Path | None,
    storage_relpath: str | None = None,
) -> ExportTransaction:
    """Wire export handlers to the provided `dispatcher`.

    Returns the :class:`ExportTransaction` backing this export. The caller is
    responsible for awaiting ``transaction.raise_if_failed()`` after the crawl so
    write/serialization failures surface as a process-level error.
    """
    crawler._cli_signal_handlers = getattr(crawler, "_cli_signal_handlers", [])

    is_storage = storage is not None
    is_path = file_path is not None

    if not (is_storage or is_path):
        raise TypeError(
            "register_export_handlers requires either a Storage instance or a Path file_path"
        )
    if is_storage and is_path:
        raise TypeError("Provide only one of storage or file_path, not both")

    if is_storage and not storage_relpath:
        raise ValueError("storage_relpath is required when passing a Storage instance")

    transaction = ExportTransaction()
    is_stdout = is_path and str(file_path).lower() in {"-", "stdout"}
    destination: Path | None = None if is_stdout else file_path

    async def _write_stdout(data: bytes) -> None:
        # stdout cannot be rolled back, but a broken pipe / closed stream must
        # still fail the transaction so the CLI exits non-zero.
        await asyncio.to_thread(_write_stdout_sync, data)

    async def _on_item_scraped(
        sender: object, item: Item, spider: Spider | None = None, **kwargs: object
    ) -> None:
        # item_scraped is the post-pipeline signal (the Crawler runs PIPELINES on
        # the item's data path), so the exporter just serializes what it receives.
        if transaction.error is not None:
            return
        local_spider = spider or sender
        try:
            try:
                data = exporter.serialize_item(item)
            except Exception as exc:
                transaction.fail(exc, "item serialization")
                return

            transaction.item_count += 1

            if is_stdout and data is not None:
                try:
                    await _write_stdout(_as_bytes(data))
                except Exception as exc:
                    transaction.fail(exc, "stdout write")
            elif data is not None and not is_stdout:
                # File/storage destinations are committed once, at close time:
                # buffer per-item chunks (NDJSON stream mode) alongside the
                # exporter's buffered document so nothing is written early.
                transaction._chunks.append(_as_bytes(data))
        except Exception as exc:
            transaction.fail(exc, f"item export for {getattr(local_spider, 'name', None)}")

    async def _on_spider_closed(
        sender: object, spider: Spider | None = None, reason: str | None = None, **kwargs: object
    ) -> None:
        local_spider = spider or sender
        if transaction.committed:
            return
        if transaction.failed:
            # A serialization/write already failed; never commit a partial or
            # empty document over the previous file.
            logger.error(
                "Skipping export commit for %s: transaction already failed",
                getattr(local_spider, "name", None),
            )
            return
        try:
            # 1. Finalize the exporter. Every accepted item is represented in the
            #    returned document exactly once.
            try:
                tail = exporter.close()
            except Exception as exc:
                transaction.fail(exc, "exporter close")
                return

            if is_stdout:
                if tail:
                    try:
                        await _write_stdout(_as_bytes(tail))
                    except Exception as exc:
                        transaction.fail(exc, "stdout write")
                        return
                transaction.committed = True
                return

            # 2. Assemble the full document. Streaming exporters (NDJSON) already
            #    emitted per-item chunks; buffered exporters (JSON/CSV/XML) emit
            #    everything at close.
            if is_storage:
                assert storage is not None
                assert storage_relpath is not None
                # Nothing was written to storage during the run; commit once.
                payload = bytearray(b"".join(transaction._chunks))
                payload.extend(_as_bytes(tail))
                try:
                    await storage.commit(bytes(payload), storage_relpath)
                except Exception as exc:
                    transaction.fail(exc, f"storage commit {storage_relpath}")
                    return
            else:
                assert destination is not None
                payload = bytearray(b"".join(transaction._chunks))
                payload.extend(_as_bytes(tail))
                try:
                    await asyncio.to_thread(_commit_file_sync, destination, payload)
                except Exception as exc:
                    transaction.fail(exc, f"file commit {destination}")
                    return

            transaction.committed = True
            logger.info(
                "Export committed %d item(s) to %s",
                transaction.item_count,
                storage_relpath if is_storage else destination,
            )
        except Exception as exc:
            transaction.fail(exc, f"export finalization for {getattr(local_spider, 'name', None)}")

    dispatcher.connect("item_scraped", _on_item_scraped, weak=False)
    dispatcher.connect("spider_closed", _on_spider_closed, weak=False)

    crawler._cli_signal_handlers.extend(
        [("item_scraped", _on_item_scraped), ("spider_closed", _on_spider_closed)]
    )
    return transaction


def _write_stdout_sync(data: bytes) -> None:
    """Write raw bytes to stdout, preferring its binary buffer."""
    import sys

    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:
        sys.stdout.write(data.decode("utf-8"))
        sys.stdout.flush()
