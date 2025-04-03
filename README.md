[![CI](https://github.com/actris-cloudnet/cloudnet-api-client/actions/workflows/test.yml/badge.svg)](https://github.com/actris-cloudnet/cloudnet-api-client/actions/workflows/test.yml)
[![PyPI version](https://badge.fury.io/py/cloudnet-api-client.svg)](https://badge.fury.io/py/cloudnet-api-client)

# Cloudnet API client

Official Python client for the [Cloudnet data portal API](https://docs.cloudnet.fmi.fi/api/data-portal.html).

## Installation

```bash
python3 -m pip install cloudnet-api-client
```

## Quickstart

```python
import cloudnet_api_client as cac

client = cac.APIClient()

sites = client.sites(type="cloudnet")
products = client.products()

metadata = client.metadata("hyytiala", "2021-01-01", product=["mwr", "radar"])
client.download(metadata, "data/")

raw_metadata = client.raw_metadata("granada", date="2024-01", instrument_id="parsivel")
client.download(raw_metadata, "data_raw/")
```

## Documentation

### `APIClient().metadata()` and `raw_metadata()` &rarr; `list[Metadata]`

Fetch product and raw file metadata from the Cloudnet data portal.

Parameters:

| name                | type                        | default | example                                              |
| ------------------- | --------------------------- | ------- | ---------------------------------------------------- |
| site_id             | `str`                       |         | "hyytiala"                                           |
| date                | `str` or `date`             | `None`  | "2024-01-01"                                         |
| date_from           | `str` or `date`             | `None`  | "2025-01-01"                                         |
| date_to             | `str` or `date`             | `None`  | "2025-01-01"                                         |
| updated_at          | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| updated_at_from     | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| updated_at_to       | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| instrument_id       | `str` or `list[str]`        | `None`  | "rpg-fmcw-94"                                        |
| instrument_pid      | `str` or `list[str]`        | `None`  | "https://hdl.handle.net/21.12132/3.191564170f8a4686" |
| product\*           | `str` or `list[str]`        | `None`  | "classification"                                     |
| show_legacy\*       | `bool`                      | `False` |                                                      |
| filename_prefix\*\* | `str` or `list[str]`        | `None`  | "stare"                                              |
| filename_suffix\*\* | `str` or `list[str]`        | `None`  | ".lv1"                                               |
| status\*\*          | `str` or `list[str]`        | `None`  | "created", "uploaded", "processed" or "invalid"      |

\* = only in `metadata()`

\*\* = only in `raw_metadata()`

**Date Handling**

The `date`, `date_from` and `date_to` parameters support:

- "YYYY-MM-DD" — a specific date
- "YYYY-MM" — the entire month
- "YYYY" — the entire year
- Or directly as `datetime.date` object

In addition to these, the `updated_at`, `updated_at_from` and `updated_at_to` parameters support:

- "YYYY-MM-DDTHH" — a specific hour
- "YYYY-MM-DDTHH:MM" — a specific minute
- "YYYY-MM-DDTHH:MM:SS" — a specific second
- "YYYY-MM-DDTHH:MM:SS.FFFFFF" — a specific microsecond
- Or directly as `datetime.datetime` object

**Return value**

Both methods return a list of `dataclass` instances, `ProductMetadata` and `RawMetadata`, respectively.

### `APIClient().filter(list[Metadata])` &rarr; `list[Metadata]`

Additional filtering of fetched metadata.

Parameters:

| name               | type                                           | default |
| ------------------ | ---------------------------------------------- | ------- |
| metadata           | `list[RawMetadata]` or `list[ProductMetadata]` |         |
| include_pattern    | `str`                                          | `None`  |
| exclude_pattern    | `str`                                          | `None`  |
| include_tag_subset | `set[str]`                                     | `None`  |
| exclude_tag_subset | `set[str]`                                     | `None`  |

### `APIClient().sites()` &rarr; `list[Site]`

Fetch cloudnet sites.

Parameters:

| name | type                 | Choices                                   | default |
| ---- | -------------------- | ----------------------------------------- | ------- |
| type | `str` or `list[str]` | "cloudnet", "campaign", "model", "hidden" | `None`  |

### `APIClient().products()` &rarr; `list[Product]`

Fetch cloudnet products.

Parameters:

| name | type                 | Choices                                   | default |
| ---- | -------------------- | ----------------------------------------- | ------- |
| type | `str` or `list[str]` | "instrument", "geophysical", "evaluation" | `None`  |

### `APIClient().instruments()` &rarr; `list[Instrument]`

Fetch cloudnet instruments.

### `APIClient().download(list[Metadata])` &rarr; `list[Path]`

Download files from the fetched metadata.

Parameters:

| name              | type                                           | default |
| ----------------- | ---------------------------------------------- | ------- |
| metadata          | `list[RawMetadata]` or `list[ProductMetadata]` |         |
| output_directory  | `PathLike` or `str`                            |         |
| concurrency_limit | `int`                                          | 5       |
| progress          | `bool` or `None`                               | `None`  |

There's also an asynchronous version of this function:
`cloudnet_api_client.adownload`. It's useful for usage inside Jupyter notebook.

## License

MIT
