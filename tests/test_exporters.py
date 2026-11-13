"""Tests for qcrawl.exporters

Unit tests for exporter classes - test serialization logic directly.

Transactional guarantees under test:
  - CSV uses a stable field set: exactly one header row, one row per item,
    even when later items introduce new fields.
  - JSON / NDJSON / XML / CSV all carry the same item count and values.
  - None and nested values are handled deterministically.
  - Every exporter reports item_count and keeps serialize_item/close as its
    public interface (buffering mode and stdout live in runner/export.py).
"""

import csv
import io

import orjson
import pytest

from qcrawl.core.item import Item
from qcrawl.exporters import CsvExporter, JsonBufferedExporter, JsonLinesExporter, XmlExporter

# JsonLinesExporter Tests


def test_jsonlines_exporter_single_item():
    """JsonLinesExporter serializes single item to NDJSON."""
    exporter = JsonLinesExporter()
    item = Item(data={"name": "test", "value": 123})

    result = exporter.serialize_item(item)

    assert result == b'{"name":"test","value":123}\n'
    assert exporter.item_count == 1


def test_jsonlines_exporter_multiple_items():
    """JsonLinesExporter serializes each item independently."""
    exporter = JsonLinesExporter()

    result1 = exporter.serialize_item(Item(data={"id": 1}))
    result2 = exporter.serialize_item(Item(data={"id": 2}))

    assert result1 == b'{"id":1}\n'
    assert result2 == b'{"id":2}\n'
    assert exporter.item_count == 2


def test_jsonlines_exporter_close_returns_empty():
    """JsonLinesExporter.close() returns empty bytes."""
    exporter = JsonLinesExporter()

    result = exporter.close()

    assert result == b""


def test_jsonlines_exporter_nested_data():
    """JsonLinesExporter handles nested data structures."""
    exporter = JsonLinesExporter()
    item = Item(data={"user": {"name": "Alice", "age": 30}, "active": True})

    result = exporter.serialize_item(item)

    assert b'"user"' in result
    assert b'"name":"Alice"' in result


# JsonBufferedExporter Tests


def test_json_buffered_exporter_buffers_items():
    """JsonBufferedExporter buffers until close; serialize_item returns None."""
    exporter = JsonBufferedExporter(buffer_size=3)

    result1 = exporter.serialize_item(Item(data={"id": 1}))
    result2 = exporter.serialize_item(Item(data={"id": 2}))

    assert result1 is None
    assert result2 is None
    assert exporter.item_count == 2


def test_json_buffered_exporter_close_emits_single_array():
    """close() emits one valid JSON array containing every buffered item."""
    exporter = JsonBufferedExporter(buffer_size=2)

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))
    result = exporter.close()

    assert result is not None
    parsed = orjson.loads(result)
    assert parsed == [{"id": 1}, {"id": 2}]


def test_json_buffered_exporter_close_flushes_remaining():
    """JsonBufferedExporter.close() flushes remaining items."""
    exporter = JsonBufferedExporter(buffer_size=5)

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))

    result = exporter.close()

    assert orjson.loads(result) == [{"id": 1}, {"id": 2}]


def test_json_buffered_exporter_close_empty_buffer():
    """An empty buffered export is a valid empty JSON array, not an empty file."""
    exporter = JsonBufferedExporter(buffer_size=5)

    result = exporter.close()

    assert result == b"[]\n"
    assert orjson.loads(result) == []


def test_json_buffered_exporter_buffer_size_is_hint_only():
    """buffer_size no longer causes mid-run flushes: one document on close."""
    exporter = JsonBufferedExporter(buffer_size=1)

    assert exporter.serialize_item(Item(data={"id": 1})) is None
    assert exporter.serialize_item(Item(data={"id": 2})) is None

    parsed = orjson.loads(exporter.close())
    assert parsed == [{"id": 1}, {"id": 2}]


def test_json_buffered_exporter_minimum_buffer_size():
    """JsonBufferedExporter enforces minimum buffer_size of 1."""
    exporter = JsonBufferedExporter(buffer_size=0)

    assert exporter.buffer_size == 1
    assert exporter.serialize_item(Item(data={"test": "value"})) is None


def test_json_buffered_exporter_nested_values_preserved():
    """Nested containers survive the buffered JSON round-trip structurally."""
    exporter = JsonBufferedExporter()
    payload = {"user": {"name": "Alice"}, "tags": ["a", "b"], "active": True}

    exporter.serialize_item(Item(data=payload))
    parsed = orjson.loads(exporter.close())

    assert parsed == [payload]


# CsvExporter Tests


def _parse_csv(blob: bytes) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(blob.decode("utf-8")))
    rows = list(reader)
    assert reader.fieldnames is not None
    return list(reader.fieldnames), rows


def test_csv_exporter_buffers_until_close():
    """serialize_item returns None; the document is produced on close."""
    exporter = CsvExporter()

    assert exporter.serialize_item(Item(data={"name": "Alice"})) is None
    blob = exporter.close()

    assert b"name" in blob
    assert b"Alice" in blob


def test_csv_exporter_single_item():
    """CsvExporter serializes single item with header on close."""
    exporter = CsvExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))

    fieldnames, rows = _parse_csv(exporter.close())

    assert fieldnames == ["age", "name"]
    assert rows == [{"age": "30", "name": "Alice"}]


def test_csv_exporter_multiple_items_single_header():
    """Multiple items produce exactly one header line and one row per item."""
    exporter = CsvExporter()

    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))
    exporter.serialize_item(Item(data={"name": "Bob", "age": "25"}))
    blob = exporter.close()

    lines = blob.decode("utf-8").strip().split("\n")
    assert len(lines) == 3  # header + 2 rows, no second header
    fieldnames, rows = _parse_csv(blob)
    assert fieldnames == ["age", "name"]
    assert [r["name"] for r in rows] == ["Alice", "Bob"]


def test_csv_exporter_later_fields_do_not_insert_second_header():
    """Regression: a field first seen on a later item must not split the file.

    The old exporter rebuilt its DictWriter mid-stream and wrote a second header
    line into the data, shifting every subsequent column. The fixed exporter
    renders a single header (the union of all fields) once, at close time.
    """
    exporter = CsvExporter()

    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))
    exporter.serialize_item(Item(data={"name": "Bob", "age": "25", "city": "NYC"}))
    exporter.serialize_item(Item(data={"name": "Cara", "age": "40", "city": "LA"}))
    blob = exporter.close()

    fieldnames, rows = _parse_csv(blob)
    assert fieldnames == ["age", "city", "name"]
    assert len(rows) == 3
    # Earlier rows get an empty cell for the field that didn't exist yet.
    assert rows[0] == {"age": "30", "city": "", "name": "Alice"}
    assert rows[1] == {"age": "25", "city": "NYC", "name": "Bob"}
    assert rows[2] == {"age": "40", "city": "LA", "name": "Cara"}
    # Exactly one physical header line in the raw document.
    assert blob.decode("utf-8").count("age,city,name\n") == 1


def test_csv_exporter_field_order_is_stable_and_sorted():
    """Field order is deterministic (sorted) regardless of arrival order."""
    exporter = CsvExporter()

    exporter.serialize_item(Item(data={"z": 1, "a": 2}))
    exporter.serialize_item(Item(data={"m": 3}))

    fieldnames, _ = _parse_csv(exporter.close())
    assert fieldnames == ["a", "m", "z"]


def test_csv_exporter_close_empty_returns_header_only():
    """With no items, close still emits a valid (empty) document shape."""
    exporter = CsvExporter()

    assert exporter.close() == b""


def test_csv_exporter_empty_values():
    """CsvExporter handles empty string values."""
    exporter = CsvExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": ""}))

    fieldnames, rows = _parse_csv(exporter.close())

    assert rows == [{"age": "", "name": "Alice"}]


def test_csv_exporter_none_values_become_empty_cells():
    """None renders as an empty cell, not the literal string 'None'."""
    exporter = CsvExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "note": None}))

    _, rows = _parse_csv(exporter.close())

    assert rows == [{"name": "Alice", "note": ""}]


def test_csv_exporter_nested_values_are_json_encoded():
    """Dicts/lists become single-line JSON text inside one cell."""
    exporter = CsvExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "meta": {"k": "v"}, "tags": ["x", "y"]}))

    _, rows = _parse_csv(exporter.close())

    assert rows[0]["meta"] == '{"k":"v"}'
    assert rows[0]["tags"] == '["x","y"]'


def test_csv_exporter_neutralizes_formula_injection():
    """CsvExporter prefixes formula-triggering string cells with a quote."""
    exporter = CsvExporter()
    exporter.serialize_item(Item(data={"formula": "=SUM(A1:A2)", "at": "@cmd", "name": "safe"}))

    text = exporter.close().decode("utf-8")

    assert "'=SUM(A1:A2)" in text
    assert "'@cmd" in text
    assert "'safe" not in text


def test_csv_exporter_item_count_matches_rows():
    """item_count equals the number of data rows written."""
    exporter = CsvExporter()

    for i in range(5):
        exporter.serialize_item(Item(data={"id": i, "extra": i % 2 and "x" or ""}))

    _, rows = _parse_csv(exporter.close())
    assert exporter.item_count == 5
    assert len(rows) == 5


# XmlExporter Tests


def test_xml_exporter_single_item():
    """XmlExporter accumulates items and outputs XML on close."""
    exporter = XmlExporter()
    item = Item(data={"name": "Alice", "age": "30"})

    exporter.serialize_item(item)

    result = exporter.close()
    assert b"<name>Alice</name>" in result


def test_xml_exporter_close_outputs_xml():
    """XmlExporter.close() outputs complete XML document, one <item> per item."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))
    exporter.serialize_item(Item(data={"name": "Bob", "age": "25"}))

    result = exporter.close()

    text = result.decode("utf-8")
    assert "<?xml version='1.0' encoding='utf-8'?>" in text
    assert text.count("<item>") == 2
    assert "<name>Alice</name>" in text
    assert "<name>Bob</name>" in text


def test_xml_exporter_empty():
    """XmlExporter.close() returns valid XML for empty data."""
    exporter = XmlExporter()

    result = exporter.close()

    text = result.decode("utf-8")
    assert "<?xml version='1.0' encoding='utf-8'?>" in text
    assert "<items/>" in text or "<items></items>" in text


def test_xml_exporter_none_values():
    """XmlExporter renders None as an empty element."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": None}))

    text = exporter.close().decode("utf-8")
    assert "<name>Alice</name>" in text
    assert "<age></age>" in text or "<age/>" in text


def test_xml_exporter_nested_values_json_encoded():
    """Nested values become JSON text rather than Python repr noise."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"user": {"name": "Alice"}, "active": True}))

    text = exporter.close().decode("utf-8")
    assert "<user>{&quot;name&quot;:&quot;Alice&quot;}</user>" in text or (
        "<user>" in text and "name" in text
    )
    assert "<active>True</active>" in text


# Cross-format consistency


def test_all_formats_same_record_count_and_values():
    """CSV, JSON, NDJSON and XML export the same N items with matching values."""
    items = [
        Item(data={"id": 1, "name": "Alice", "note": None}),
        Item(data={"id": 2, "name": "Bob", "note": "hi", "city": "NYC"}),
        Item(data={"id": 3, "name": "Cara", "tags": ["a", "b"]}),
    ]

    # NDJSON
    nd = JsonLinesExporter()
    nd_blob = b"".join(nd.serialize_item(it) for it in items) + nd.close()
    nd_records = [orjson.loads(line) for line in nd_blob.strip().split(b"\n")]

    # JSON array
    js = JsonBufferedExporter()
    for it in items:
        js.serialize_item(it)
    js_records = orjson.loads(js.close())

    # CSV
    cv = CsvExporter()
    for it in items:
        cv.serialize_item(it)
    cv_blob = cv.close()
    cv_rows = list(csv.DictReader(io.StringIO(cv_blob.decode("utf-8"))))

    # XML
    xm = XmlExporter()
    for it in items:
        xm.serialize_item(it)
    xm_blob = xm.close()

    # Equal record counts everywhere.
    assert len(nd_records) == len(js_records) == len(cv_rows) == 3
    assert xm_blob.count(b"<item>") == 3
    assert nd.item_count == js.item_count == cv.item_count == xm.item_count == 3

    # Values line up per record.
    for ndr, jsr, cvr in zip(nd_records, js_records, cv_rows, strict=True):
        assert ndr == jsr
        assert cvr["id"] == str(ndr["id"])
        assert cvr["name"] == ndr["name"]

    # The late field is present in the CSV header and empty on earlier rows.
    assert "city" in cv_blob.decode("utf-8").split("\n")[0]
    assert cv_rows[0]["city"] == ""
    assert cv_rows[1]["city"] == "NYC"


# Protocol Conformance Tests


def test_all_exporters_implement_protocol():
    """All exporter classes conform to Exporter protocol."""
    from qcrawl.exporters import Exporter

    assert isinstance(JsonLinesExporter(), Exporter)
    assert isinstance(JsonBufferedExporter(), Exporter)
    assert isinstance(CsvExporter(), Exporter)
    assert isinstance(XmlExporter(), Exporter)


def test_exporter_protocol_methods():
    """Exporter protocol defines required methods and item_count."""
    from qcrawl.exporters import Exporter

    assert hasattr(Exporter, "serialize_item")
    assert hasattr(Exporter, "close")
    assert "item_count" in Exporter.__annotations__


@pytest.mark.parametrize(
    "exporter",
    [JsonLinesExporter(), JsonBufferedExporter(), CsvExporter(), XmlExporter()],
)
def test_all_exporters_track_item_count(exporter):
    """Every exporter starts at zero and counts accepted items."""
    assert exporter.item_count == 0
    exporter.serialize_item(Item(data={"a": 1}))
    exporter.serialize_item(Item(data={"b": 2}))
    assert exporter.item_count == 2
