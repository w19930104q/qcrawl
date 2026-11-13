from __future__ import annotations

import csv
import io
from typing import Protocol, runtime_checkable

import lxml.etree as ET
import orjson

from qcrawl.core.item import Item

# Characters that spreadsheet apps (Excel, Sheets, LibreOffice) interpret as the
# start of a formula. A CSV cell beginning with one of these is a formula-
# injection vector, so it is prefixed with a single quote to neutralize it.
_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def _item_data(item: Item) -> dict[str, object]:
    """Return the item's data mapping as a plain dict."""
    data = item.data if hasattr(item, "data") else item
    return dict(data)


def _flatten_scalar(value: object) -> object:
    """Normalize a field value for tabular/string output (CSV cells, XML text).

    Scalars (str, int, float, bool) pass through unchanged so numeric values keep
    their native representation. ``None`` becomes an empty string, which both the
    CSV writer and XML element text render as an empty cell/element. Containers
    (dict/list/tuple) are serialized to compact JSON so the value is stable,
    one-line and identical regardless of where it is rendered.
    """
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list, tuple)):
        return orjson.dumps(value).decode("utf-8")
    return str(value)


def _neutralize_csv_value(value: object) -> object:
    """Prefix formula-triggering string cells with a quote; pass others through."""
    if isinstance(value, str) and value and value[0] in _CSV_FORMULA_PREFIXES:
        return "'" + value
    return value


@runtime_checkable
class Exporter(Protocol):
    """Structural protocol for qcrawl exporters.

    An export is a commit-able transaction: ``serialize_item`` accumulates items
    without touching the destination, and ``close`` produces the complete document
    in one final chunk. Destinations only persist that final chunk atomically, so
    a failed run can never leave a partial file behind.

    ``item_count`` reports how many items the exporter has accepted; every
    accepted item appears exactly once in the output produced by ``close``.
    """

    item_count: int

    def serialize_item(self, item: Item) -> bytes | str | None:
        """Serialize a single item and return data to write (or None if buffered)."""
        ...

    def close(self) -> bytes | None:
        """Return final chunk of data on spider close (e.g. closing array, footer)."""
        ...


class JsonLinesExporter:
    """NDJSON exporter (one JSON object per line).

    Behavior:
      - Each `serialize_item` call returns a single newline-terminated JSON line as bytes.
      - `close()` is a no-op and returns empty bytes.
      - Nested values are serialized natively by orjson.
    """

    def __init__(self) -> None:
        self.item_count = 0

    def serialize_item(self, item: Item) -> bytes:
        data = _item_data(item)
        self.item_count += 1
        result: bytes = orjson.dumps(data)
        return result + b"\n"

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        return b""


class JsonBufferedExporter:
    """Buffered JSON exporter that emits one valid JSON array on close.

    Args:
        buffer_size (int): Hint kept for interface compatibility; items are
            accumulated in memory regardless, because a single JSON array with a
            stable field set can only be committed as one document.

    Behavior:
      - Items are buffered in-memory; `serialize_item` always returns `None`.
      - `close()` serializes every buffered item as a single JSON array with
        `orjson.OPT_INDENT_2` (or `b"[]\\n"` when no items were seen).
    """

    def __init__(self, buffer_size: int = 500) -> None:
        # Accepted for interface compatibility; the document is committed whole.
        self.buffer_size = max(1, int(buffer_size))
        self.buffer: list[object] = []
        self.item_count = 0

    def serialize_item(self, item: Item) -> None:
        data = _item_data(item)
        self.buffer.append(data)
        self.item_count += 1
        return None

    def close(self) -> bytes:
        """Finalize export: emit the whole buffer as one JSON array."""
        if not self.buffer:
            return b"[]\n"
        out: bytes = orjson.dumps(self.buffer, option=orjson.OPT_INDENT_2) + b"\n"
        self.buffer.clear()
        return out


class CsvExporter:
    """CSV exporter with a stable field set and a single header row.

    The exporter is fully buffered: items are accumulated across the whole spider
    run and the document is produced once in `close()`. The header is therefore
    written exactly once, after every item has been seen, and contains the union
    of all item keys. Fields appearing only on later items get empty cells on the
    earlier rows instead of forcing a second header into the data stream.

    Methods:
        serialize_item(item) -> None
            Buffer an item (returns None; nothing is written until close).
        close() -> bytes
            Emit header + exactly one row per buffered item, UTF-8 encoded.
    """

    def __init__(self) -> None:
        self.items: list[dict[str, object]] = []
        self._fieldnames: set[str] = set()
        self.item_count = 0

    def serialize_item(self, item: Item) -> None:
        data = _item_data(item)
        # Track the stable field set; the header is rendered once, at close time.
        self._fieldnames.update(data.keys())
        self.items.append(data)
        self.item_count += 1
        return None

    def close(self) -> bytes:
        """Render the complete CSV document: one header, one row per item."""
        if not self.items:
            return b""
        output = io.StringIO()
        fieldnames = sorted(self._fieldnames)
        writer = csv.DictWriter(
            output, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()

        for data in self.items:
            row = {
                key: _neutralize_csv_value(_flatten_scalar(value)) for key, value in data.items()
            }
            # Missing keys write as empty cells (restval default), never a header.
            writer.writerow(row)

        self.items.clear()
        self._fieldnames.clear()
        return output.getvalue().encode("utf-8")


class XmlExporter:
    """XML exporter that accumulates items and writes them as XML elements.

    Behavior:
      - Items are collected in-memory and emitted as an XML document on `close()`.
      - `serialize_item` returns None while accumulating.
      - `close()` returns UTF-8 encoded XML bytes (pretty-printed using lxml).
      - None renders as an empty element; nested containers are JSON-encoded text.
    """

    def __init__(self) -> None:
        self.items: list[dict[str, object]] = []
        self.item_count = 0

    def serialize_item(self, item: Item) -> None:
        data = _item_data(item)
        self.items.append(data)
        self.item_count += 1
        return None

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        root = ET.Element("items")

        for data in self.items:
            item_elem = ET.SubElement(root, "item")
            for k, v in data.items():
                child = ET.SubElement(item_elem, str(k))
                flat = _flatten_scalar(v)
                child.text = "" if flat == "" else str(flat)

        self.items.clear()
        return (
            ET.tostring(
                root,
                encoding="utf-8",
                pretty_print=True,
                xml_declaration=True,
            )
            + b"\n"
        )
