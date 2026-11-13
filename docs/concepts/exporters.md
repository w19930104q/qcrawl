
After a spider extracts data from web-page it's often necessary to store that data in a structured format
for further analysis or processing.

qCrawl provides exporters that serialize scraped items and write them (streamed or buffered) to a destination.
They can be used via the CLI or programmatically.

### Transactional output

Each spider run is committed as **one transaction**:

- Items are serialized in memory while the crawl runs; nothing is written to the
  destination until the spider closes.
- On close, the complete document is staged in a temporary file and atomically
  renamed into place (`os.replace`). A failed or interrupted run therefore can
  never leave a half-written file, and a previously complete output file is
  kept byte-identical until the new document is fully written and fsynced.
- Serialization or write failures are no longer swallowed: they surface as an
  `ExportError` from the runner, and the CLI exits with status `1`.
- Every scraped item appears exactly once in the output.
- stdout (`-` / `stdout`) remains a streaming destination; it cannot be rolled
  back, but write failures still fail the run.

**Format comparison:**

| Format     | Streaming  | Best For                            | File Size  |
|------------|------------|-------------------------------------|------------|
| **NDJSON** | ✅ Yes (stdout) / committed whole (file) | Large datasets, streaming pipelines | Medium     |
| **JSON**   | ❌ No (one array, committed at close)    | API responses, small datasets       | Medium     |
| **CSV**    | ❌ No (single stable header, committed at close) | Excel, data analysis, flat data | Small      |
| **XML**    | ❌ No       | Legacy systems, SOAP APIs           | Large      |



## Examples - CLI usage

### JSON Lines (default)
```bash
# One JSON object per line (streaming-friendly, memory efficient)
qcrawl quotes_css_spider:Quotes --export output.jsonl

# Or explicitly specify format
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-format ndjson
```

**Output (output.jsonl):**
```json
{"url": "https://quotes.toscrape.com/", "text": "The world as we...", "author": "Albert Einstein", "tags": ["change", "world"]}
{"url": "https://quotes.toscrape.com/", "text": "It is our choices...", "author": "J.K. Rowling", "tags": ["abilities", "choices"]}
```

### JSON (full array)
```bash
# All items in a single JSON array
qcrawl quotes_css_spider:Quotes --export output.json --export-format json
```

**Output (output.json):**
```json
[
  {
    "url": "https://quotes.toscrape.com/",
    "text": "The world as we...",
    "author": "Albert Einstein",
    "tags": ["change", "world"]
  },
  {
    "url": "https://quotes.toscrape.com/",
    "text": "It is our choices...",
    "author": "J.K. Rowling",
    "tags": ["abilities", "choices"]
  }
]
```

### CSV
```bash
# Comma-separated values (works best with flat data structures)
qcrawl quotes_css_spider:Quotes --export output.csv --export-format csv
```

The CSV exporter buffers every item and writes the document once on close, so
the header is always a single row containing the **union of all item fields**
in sorted order. A field that first appears on a later item gets empty cells on
the earlier rows — no second header is ever inserted into the data. Value rules:

- `None` / missing fields render as empty cells;
- dicts and lists are JSON-encoded into a single cell (one line, no quoting
  surprises);
- strings beginning with `=`, `+`, `-`, `@`, tab or CR are prefixed with a
  single quote to neutralize spreadsheet formula injection.

**Output (output.csv):**
```csv
url,text,author,tags
https://quotes.toscrape.com/,"The world as we...",Albert Einstein,"[""change"", ""world""]"
https://quotes.toscrape.com/,"It is our choices...",J.K. Rowling,"[""abilities"", ""choices""]"
```

### XML
```bash
# XML format
qcrawl quotes_css_spider:Quotes --export output.xml --export-format xml
```

**Output (output.xml):**
```xml
<?xml version="1.0" encoding="utf-8"?>
<items>
  <item>
    <url>https://quotes.toscrape.com/</url>
    <text>The world as we...</text>
    <author>Albert Einstein</author>
    <tags>['change', 'world']</tags>
  </item>
</items>
```

The XML exporter writes each field value via `str(value)`; `None` renders as an
empty element, and non-scalar values (lists, dicts) are JSON-encoded rather than
expanded into child elements. Flatten or pre-format such fields in a pipeline if
you need structured XML.

### Streaming vs Buffered Mode

```bash
# Buffered (default): all formats accumulate items and commit one document at close
qcrawl quotes_css_spider:Quotes --export output.json --export-mode buffered

# Stream mode for JSON: emit NDJSON lines (only affects the json format;
# file destinations still commit atomically on close, stdout stays live)
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-mode stream

# Adjust buffer size (items to collect before serializing)
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-buffer-size 1000
```



## Examples - Programmatic usage

```python hl_lines="3"
# Async usage (recommended)
import asyncio
from quotes_spider import QuotesSpider # replace with your spider
from qcrawl.runner import SpiderRunner

runner = SpiderRunner(
    settings={
        "export": "exports/quotes.ndjson",
        "export_format": "ndjson",
        "export_mode": "buffered",
        "export_buffer_size": 500,
        "log_level": "INFO",
        "concurrency": 50,
    }
)

async def main() -> None:
    # Await the async entrypoint from your event loop
    await runner.crawl(QuotesSpider)

if __name__ == "__main__":
    asyncio.run(main())


# Synchronous convenience (for simple scripts)
from quotes_spider import QuotesSpider  # replace with your spider
from qcrawl.runner import SpiderRunner

runner = SpiderRunner(
    settings={
        "export": "exports/quotes.ndjson",
        "export_format": "ndjson",
        "log_level": "INFO",
        "concurrency": 50,
    }
)

# Blocks and is implemented via asyncio.run(); raises if an event loop is already running.
runner.crawl_sync(QuotesSpider)
```
