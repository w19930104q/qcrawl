"""Tests for qcrawl.runner.export - Exporter factory and export transactions

Tests focus on the following behavior:
- Exporter selection based on format (ndjson, json, csv, xml)
- Buffer size and mode parameter handling
- Export handler registration with signal system
- File, storage and stdout output integration
- Transactional commit: failures propagate to the ExportTransaction and a failed
  run never overwrites a previously complete file.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from qcrawl.core.item import Item
from qcrawl.runner.export import (
    ExportError,
    ExportTransaction,
    build_exporter,
    register_export_handlers,
)
from qcrawl.signals import SignalRegistry
from qcrawl.storage import FileStorage

# Exporter Selection Tests


def test_build_exporter_ndjson():
    """build_exporter returns JsonLinesExporter for ndjson format."""
    exporter = build_exporter("ndjson")

    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    assert result == b'{"test":"value"}\n', "Should produce NDJSON format"


def test_build_exporter_json_buffered():
    """build_exporter returns a buffering JSON exporter for json+buffered."""
    exporter = build_exporter("json", mode="buffered", buffer_size=2)

    item1 = Item(data={"id": 1})
    item2 = Item(data={"id": 2})

    # Buffered exporters accumulate and return None until close.
    assert exporter.serialize_item(item1) is None
    assert exporter.serialize_item(item2) is None

    import orjson

    assert orjson.loads(exporter.close()) == [{"id": 1}, {"id": 2}]


def test_build_exporter_json_stream():
    """build_exporter returns JsonLinesExporter for json+stream mode."""
    exporter = build_exporter("json", mode="stream")

    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    assert result == b'{"test":"value"}\n', "Should produce NDJSON in stream mode"


def test_build_exporter_csv():
    """build_exporter returns CsvExporter for csv format (buffered)."""
    exporter = build_exporter("csv")

    # CSV now buffers; the document appears on close.
    assert exporter.serialize_item(Item(data={"name": "test", "value": "123"})) is None
    blob = exporter.close()

    assert b"name,value" in blob
    assert b"test" in blob and b"123" in blob


def test_build_exporter_xml():
    """build_exporter returns XmlExporter for xml format."""
    exporter = build_exporter("xml")

    item = Item(data={"test": "value"})
    exporter.serialize_item(item)
    result = exporter.close()

    assert result is not None
    assert b"<items>" in result
    assert b"<test>value</test>" in result


@pytest.mark.parametrize(
    "format_name,expected_contains",
    [
        ("ndjson", b'{"test":"value"}\n'),
        ("NDJSON", b'{"test":"value"}\n'),  # Case insensitive
        ("json", None),  # Buffered returns None until close
    ],
)
def test_build_exporter_case_insensitive(format_name, expected_contains):
    """build_exporter handles format names case-insensitively."""
    exporter = build_exporter(format_name)
    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    if expected_contains:
        assert result == expected_contains
    else:
        assert result is None


def test_build_exporter_unknown_format():
    """build_exporter raises ValueError for unknown format."""
    with pytest.raises(ValueError, match="Unknown export format: 'invalid'"):
        build_exporter("invalid")


def test_build_exporter_none_format():
    """build_exporter raises ValueError for None format."""
    with pytest.raises(ValueError, match="Unknown export format: None"):
        build_exporter(None)


# Buffer Size Tests


def test_build_exporter_custom_buffer_size():
    """build_exporter passes buffer_size through to the buffered JSON exporter."""
    exporter = build_exporter("json", mode="buffered", buffer_size=3)
    assert exporter.buffer_size == 3

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))
    assert exporter.serialize_item(Item(data={"id": 3})) is None

    import orjson

    assert orjson.loads(exporter.close()) == [{"id": 1}, {"id": 2}, {"id": 3}]


# Mode Parameter Tests


@pytest.mark.parametrize("mode", ["buffered", "BUFFERED", "Buffered"])
def test_build_exporter_mode_case_insensitive(mode):
    """build_exporter handles mode parameter case-insensitively."""
    exporter = build_exporter("json", mode=mode, buffer_size=1)

    assert exporter.serialize_item(Item(data={"test": "value"})) is None
    assert exporter.close() is not None


# Export Handler Integration Tests


def _registry_and_crawler():
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])
    return dispatcher, crawler


@pytest.mark.asyncio
async def test_register_export_handlers_file_export(tmp_path):
    """register_export_handlers commits items to file on spider_closed."""
    output_file = tmp_path / "output.ndjson"
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    test_item = Item(data={"name": "test", "value": 123})
    await dispatcher.send_async("item_scraped", item=test_item, spider=None)

    # Nothing is written before the transaction commits.
    assert not output_file.exists()

    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    assert output_file.exists()
    assert transaction.committed is True
    transaction.raise_if_failed()  # must not raise

    content = output_file.read_text()
    assert "test" in content and "123" in content


@pytest.mark.asyncio
async def test_register_export_handlers_stdout_export(capsys):
    """register_export_handlers streams items to stdout when file_path is '-'."""
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=Path("-"),
    )

    test_item = Item(data={"output": "stdout"})
    await dispatcher.send_async("item_scraped", item=test_item, spider=None)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    captured = capsys.readouterr()
    assert "stdout" in captured.out
    transaction.raise_if_failed()


@pytest.mark.asyncio
async def test_register_export_handlers_multiple_items(tmp_path):
    """Every scraped item ends up in the committed file exactly once."""
    output_file = tmp_path / "multi.ndjson"
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    items = [
        Item(data={"id": 1, "name": "first"}),
        Item(data={"id": 2, "name": "second"}),
        Item(data={"id": 3, "name": "third"}),
    ]
    for item in items:
        await dispatcher.send_async("item_scraped", item=item, spider=None)

    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    lines = [line for line in output_file.read_text().split("\n") if line.strip()]
    assert len(lines) == 3
    assert "first" in output_file.read_text()


@pytest.mark.asyncio
async def test_register_export_handlers_buffered_json(tmp_path):
    """Buffered JSON commits one valid JSON array on close."""
    import orjson

    output_file = tmp_path / "buffered.json"
    exporter = build_exporter("json", mode="buffered", buffer_size=2)
    dispatcher, crawler = _registry_and_crawler()

    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)
    await dispatcher.send_async("item_scraped", item=Item(data={"id": 2}), spider=None)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    assert orjson.loads(output_file.read_bytes()) == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_register_export_handlers_csv_stable_header(tmp_path):
    """End-to-end CSV: later fields yield one header and aligned rows."""
    import csv as csv_mod
    import io

    output_file = tmp_path / "out.csv"
    exporter = build_exporter("csv")
    dispatcher, crawler = _registry_and_crawler()

    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    await dispatcher.send_async(
        "item_scraped", item=Item(data={"name": "Alice", "age": "30"}), spider=None
    )
    await dispatcher.send_async(
        "item_scraped",
        item=Item(data={"name": "Bob", "age": "25", "city": "NYC"}),
        spider=None,
    )
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    rows = list(csv_mod.DictReader(io.StringIO(output_file.read_text())))
    assert rows == [
        {"age": "30", "city": "", "name": "Alice"},
        {"age": "25", "city": "NYC", "name": "Bob"},
    ]


@pytest.mark.asyncio
async def test_register_export_handlers_storage_commit(tmp_path):
    """Storage destinations are written via one atomic commit on close."""
    storage = FileStorage(root=tmp_path / "store")
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=storage,
        file_path=None,
        storage_relpath="nested/output.ndjson",
    )

    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)
    # No partial writes during the run.
    assert not (tmp_path / "store" / "nested").exists()

    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    target = tmp_path / "store" / "nested" / "output.ndjson"
    assert target.exists()
    assert target.read_text().strip() == '{"id":1}'
    assert transaction.committed is True


class _FailingStorage(FileStorage):
    """Storage whose commit always raises."""

    async def commit(self, data, path):  # type: ignore[override]
        raise OSError("simulated storage outage")


@pytest.mark.asyncio
async def test_storage_write_failure_propagates(tmp_path):
    """A failing storage commit marks the transaction failed and re-raises."""
    storage = _FailingStorage(root=tmp_path / "store")
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=storage,
        file_path=None,
        storage_relpath="out.ndjson",
    )

    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    assert transaction.committed is False
    with pytest.raises(ExportError, match="Export failed"):
        transaction.raise_if_failed()


@pytest.mark.asyncio
async def test_file_write_failure_propagates_and_keeps_old_file(tmp_path, monkeypatch):
    """A failed commit leaves the previous complete file byte-identical."""
    output_file = tmp_path / "out.ndjson"
    previous = b'{"previous":true}\n'
    output_file.write_bytes(previous)

    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)

    def boom(destination, payload):
        raise OSError("disk full")

    monkeypatch.setattr("qcrawl.runner.export._commit_file_sync", boom)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    assert transaction.committed is False
    with pytest.raises(ExportError):
        transaction.raise_if_failed()
    # The old file must be untouched and no temp files left behind.
    assert output_file.read_bytes() == previous
    assert not list(output_file.parent.glob(".*.tmp"))


@pytest.mark.asyncio
async def test_serialization_failure_marks_transaction_failed(tmp_path):
    """An exporter raising during serialize_item fails the whole transaction."""
    output_file = tmp_path / "out.ndjson"

    class BrokenExporter:
        item_count = 0

        def serialize_item(self, item):
            raise ValueError("cannot serialize")

        def close(self):
            return b""

    dispatcher, crawler = _registry_and_crawler()
    transaction = register_export_handlers(
        dispatcher=dispatcher,
        exporter=BrokenExporter(),  # type: ignore[arg-type]
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    with pytest.raises(ExportError):
        transaction.raise_if_failed()
    assert not output_file.exists()


@pytest.mark.asyncio
async def test_rerun_replaces_file_with_fresh_document(tmp_path):
    """A second successful run replaces (not appends to) the first run's file."""
    output_file = tmp_path / "out.ndjson"
    dispatcher, crawler = _registry_and_crawler()

    for payload in ([{"id": 1}], [{"id": 2}, {"id": 3}]):
        exporter = build_exporter("ndjson")
        dispatcher, crawler = _registry_and_crawler()
        transaction = register_export_handlers(
            dispatcher=dispatcher,
            exporter=exporter,
            crawler=crawler,
            storage=None,
            file_path=output_file,
        )
        for row in payload:
            await dispatcher.send_async("item_scraped", item=Item(data=row), spider=None)
        await dispatcher.send_async("spider_closed", spider=None, reason="finished")
        transaction.raise_if_failed()

    lines = [ln for ln in output_file.read_text().splitlines() if ln.strip()]
    assert lines == ['{"id":2}', '{"id":3}']


def test_transaction_records_first_error_only():
    """Only the first failure is retained as the cause; later ones are logged."""
    txn = ExportTransaction()
    first = OSError("first")
    txn.fail(first, "phase 1")
    txn.fail(RuntimeError("second"), "phase 2")

    assert txn.error is first
    with pytest.raises(ExportError) as exc_info:
        txn.raise_if_failed()
    assert exc_info.value.__cause__ is first


@pytest.mark.asyncio
async def test_register_export_handlers_requires_path_or_storage():
    """register_export_handlers raises TypeError when neither file_path nor storage provided."""
    exporter = build_exporter("ndjson")
    dispatcher, crawler = _registry_and_crawler()

    with pytest.raises(
        TypeError,
        match="register_export_handlers requires either a Storage instance or a Path file_path",
    ):
        register_export_handlers(
            dispatcher=dispatcher,
            exporter=exporter,
            crawler=crawler,
            storage=None,
            file_path=None,
        )
