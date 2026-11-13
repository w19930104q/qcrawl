"""End-to-end export transaction tests using a fully local spider.

No network or Docker: a fake in-process download handler feeds canned Pages to a
real Spider/Crawler/engine, and the runner's export machinery commits the
results. These tests pin the transactional contract:

- heterogeneous items (fields appearing only on later items), None and nested
  values export with one header / one row per item and identical record counts
  across CSV, JSON, NDJSON and XML;
- a write failure propagates out of the runner and leaves the previous complete
  file untouched;
- a re-run replaces the old document instead of appending to it;
- stdout export keeps working.
"""

import argparse
import csv
import io
from types import SimpleNamespace

import orjson
import pytest

from qcrawl.core.response import Page
from qcrawl.core.spider import Spider
from qcrawl.runner.engine import run
from qcrawl.runner.export import ExportError
from qcrawl.settings import Settings
from qcrawl.signals import signals_dispatcher

# --- Local plumbing: a fake downloader so the crawl needs no network ---------


class LocalDownloader:
    """DownloaderProtocol stand-in returning a canned local Page."""

    def __init__(self) -> None:
        self.signals = signals_dispatcher

    async def fetch(self, request, *, spider=None, headers=None, timeout=180.0):
        return Page(
            url=getattr(request, "url", "http://local.test/"),
            content=b"<html></html>",
            status_code=200,
            headers={},
            request=request,
        )

    async def close(self) -> None:
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# The handler manager resolves DOWNLOAD_HANDLERS values via dotted module paths.
_HANDLER_PATH = f"{LocalDownloader.__module__}.LocalDownloader"


class HeterogeneousSpider(Spider):
    """Yields items whose field set grows after the first item."""

    name = "heterogeneous"
    start_urls = ["http://local.test/a", "http://local.test/b", "http://local.test/c"]

    custom_settings = {"DOWNLOAD_HANDLERS": {"http": _HANDLER_PATH, "https": _HANDLER_PATH}}

    async def parse(self, response):
        idx = self.start_urls.index(response.url)
        if idx == 0:
            yield {"id": 1, "name": "Alice", "note": None}
        elif idx == 1:
            # New field `city` appears for the first time on the second item.
            yield {"id": 2, "name": "Bob", "note": "ok", "city": "NYC"}
        else:
            # Nested value plus another late field.
            yield {
                "id": 3,
                "name": "Cara",
                "note": "fine",
                "city": "LA",
                "meta": {"role": "admin", "tags": ["x", "y"]},
            }


class EmptySpider(Spider):
    """Parses successfully but yields no items."""

    name = "empty"
    start_urls = ["http://local.test/empty"]

    custom_settings = {"DOWNLOAD_HANDLERS": {"http": _HANDLER_PATH, "https": _HANDLER_PATH}}

    async def parse(self, response):
        # Async generator that yields nothing.
        if False:  # pragma: no cover
            yield


def _args(export: str | None, fmt: str, **overrides) -> argparse.Namespace:
    ns = argparse.Namespace(
        export=export,
        export_format=fmt,
        export_mode="buffered",
        export_buffer_size=500,
        setting=[],
        settings_file=None,
        log_level="ERROR",
        log_file=None,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


def _settings() -> Settings:
    return Settings().with_overrides(
        {"DOWNLOAD_HANDLERS": {"http": _HANDLER_PATH, "https": _HANDLER_PATH}}
    )


@pytest.fixture(autouse=True)
def _clean_global_signals():
    """Each test starts with a clean global signal registry."""
    for name in ("item_scraped", "spider_closed"):
        signals_dispatcher.disconnect_all(name)
    yield
    for name in ("item_scraped", "spider_closed"):
        signals_dispatcher.disconnect_all(name)


# --- Cross-format consistency with a real local spider -----------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", ["csv", "json", "ndjson", "xml"])
async def test_local_spider_all_formats_same_records(tmp_path, fmt):
    """Every format exports 3 items with matching values and no column drift."""
    output = tmp_path / f"out.{fmt}"
    args = _args(str(output), fmt)

    await run(HeterogeneousSpider, args, SimpleNamespace(spider_args={}), _settings())

    blob = output.read_bytes()
    assert blob, "a document must be committed even for small runs"

    if fmt == "csv":
        text = blob.decode("utf-8")
        lines = text.strip().split("\n")
        # One header + exactly one row per item; no second header in the data.
        assert len(lines) == 4
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        assert reader.fieldnames == ["city", "id", "meta", "name", "note"]
        assert len(rows) == 3
        assert rows[0]["city"] == "" and rows[0]["note"] == ""  # None/missing -> empty
        assert rows[1]["city"] == "NYC"
        assert rows[2]["meta"] == '{"role":"admin","tags":["x","y"]}'
        # Numeric values are preserved as their text form.
        assert [r["id"] for r in rows] == ["1", "2", "3"]
    elif fmt == "json":
        records = orjson.loads(blob)
        assert len(records) == 3
        assert records[1]["city"] == "NYC"
        assert records[2]["meta"]["tags"] == ["x", "y"]
    elif fmt == "ndjson":
        records = [orjson.loads(line) for line in blob.strip().split(b"\n")]
        assert len(records) == 3
        assert records[0] == {"id": 1, "name": "Alice", "note": None}
    else:  # xml
        text = blob.decode("utf-8")
        assert text.count("<item>") == 3
        assert "<city>NYC</city>" in text
        assert "<note></note>" in text or "<note/>" in text


@pytest.mark.asyncio
async def test_local_spider_csv_header_physically_once(tmp_path):
    """The raw CSV bytes contain exactly one header line (the original bug)."""
    output = tmp_path / "out.csv"
    await run(
        HeterogeneousSpider,
        _args(str(output), "csv"),
        SimpleNamespace(spider_args={}),
        _settings(),
    )

    header = "city,id,meta,name,note\n"
    assert output.read_text().count(header) == 1


# --- Empty run still commits a valid document --------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fmt,expected",
    [("json", b"[]\n"), ("ndjson", b""), ("xml", None), ("csv", b"")],
)
async def test_local_spider_empty_run_commits_valid_document(tmp_path, fmt, expected):
    output = tmp_path / f"empty.{fmt}"
    await run(
        EmptySpider,
        _args(str(output), fmt),
        SimpleNamespace(spider_args={}),
        _settings(),
    )

    assert output.exists()
    blob = output.read_bytes()
    if expected is not None:
        assert blob == expected
    else:
        assert b"<items" in blob and b"<item>" not in blob


# --- Write failure propagation and previous-file preservation ----------------


@pytest.mark.asyncio
async def test_local_spider_write_failure_raises_and_keeps_old_file(tmp_path, monkeypatch):
    """A commit failure surfaces as ExportError; the prior good file survives."""
    output = tmp_path / "out.ndjson"
    previous = b'{"good":"previous run"}\n'
    output.write_bytes(previous)

    def boom(destination, payload):
        raise OSError("simulated disk failure")

    monkeypatch.setattr("qcrawl.runner.export._commit_file_sync", boom)

    with pytest.raises(ExportError, match="Export failed"):
        await run(
            HeterogeneousSpider,
            _args(str(output), "ndjson"),
            SimpleNamespace(spider_args={}),
            _settings(),
        )

    assert output.read_bytes() == previous
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.asyncio
async def test_local_spider_storage_failure_raises_and_keeps_old_file(tmp_path, monkeypatch):
    """Same guarantee through the Storage.commit path."""
    from tests.runner.test_export import _FailingStorage  # reuse test double

    good = tmp_path / "store" / "out.ndjson"
    good.parent.mkdir(parents=True, exist_ok=True)
    good.write_bytes(b'{"good":true}\n')

    monkeypatch.setattr("qcrawl.runner.engine.FileStorage", lambda root: _FailingStorage(root=root))

    class StorageSpider(HeterogeneousSpider):
        custom_settings = {
            "DOWNLOAD_HANDLERS": {"http": _HANDLER_PATH, "https": _HANDLER_PATH},
            "STORAGE": {"backend": "file", "path": str(good)},
        }

    with pytest.raises(ExportError):
        await run(
            StorageSpider,
            _args(None, "ndjson"),
            SimpleNamespace(spider_args={}),
            _settings(),
        )

    assert good.read_bytes() == b'{"good":true}\n'


# --- Re-run replaces instead of appending ------------------------------------


@pytest.mark.asyncio
async def test_local_spider_rerun_replaces_output(tmp_path):
    """Running twice leaves exactly the second run's records in the file."""
    output = tmp_path / "out.csv"

    await run(
        HeterogeneousSpider,
        _args(str(output), "csv"),
        SimpleNamespace(spider_args={}),
        _settings(),
    )
    first = output.read_bytes()
    assert first.count(b"\n") == 4  # header + 3 rows

    class TwoItemSpider(HeterogeneousSpider):
        start_urls = ["http://local.test/a", "http://local.test/b"]

    await run(
        TwoItemSpider,
        _args(str(output), "csv"),
        SimpleNamespace(spider_args={}),
        _settings(),
    )

    second = output.read_bytes()
    assert second != first
    assert second.count(b"\n") == 3  # header + 2 rows, nothing appended
    rows = list(csv.DictReader(io.StringIO(second.decode("utf-8"))))
    assert len(rows) == 2
    assert [r["name"] for r in rows] == ["Alice", "Bob"]


# --- Stdout remains supported -------------------------------------------------


@pytest.mark.asyncio
async def test_local_spider_stdout_export_still_works(capsys):
    """stdout destination keeps streaming output and a clean transaction."""
    await run(
        HeterogeneousSpider,
        _args("-", "ndjson"),
        SimpleNamespace(spider_args={}),
        _settings(),
    )

    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    assert len(lines) == 3
    assert orjson.loads(lines[0])["name"] == "Alice"
