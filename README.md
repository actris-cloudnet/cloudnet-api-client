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
from cloudnet_api_client import APIClient

client = APIClient()

sites = client.sites()
site = client.site("hyytiala")

products = client.products()
product = client.products("classification")

models = client.models()
model = client.model("ecmwf-open")

instruments = client.instruments()
instrument = client.instrument("d6bf209b-c48b-48a4-bbfb-fed713b27832")

file = client.file("405cc410-1f24-4ea9-bae8-da7f22be26cb")

files = client.files(site_id="hyytiala", date="2021-01-01", product_id=["mwr", "radar"])
file_paths = client.download(files, "data/")

raw_files = client.raw_files(site_id="granada", date="2024-01", instrument_id="parsivel")
file_paths = client.download(raw_files, "data_raw/")
```

When downloading files inside Jupyter notebook (or similar environment), you have to use the asynchronous version:

```python
file_paths = await client.adownload(metadata)
```

## Documentation

### `APIClient().files()` and `raw_files()` &rarr; `list[Metadata]`

Fetch product and raw file metadata from the Cloudnet data portal.

Parameters:

| name                | type                        | default | example                                              |
| ------------------- | --------------------------- | ------- | ---------------------------------------------------- |
| site_id             | `str` or `list[str]`        | `None`  | "hyytiala"                                           |
| date                | `str` or `date`             | `None`  | "2024-01-01"                                         |
| date_from           | `str` or `date`             | `None`  | "2025-01-01"                                         |
| date_to             | `str` or `date`             | `None`  | "2025-01-01"                                         |
| updated_at          | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| updated_at_from     | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| updated_at_to       | `str`, `date` or `datetime` | `None`  | "2025-01-01T12:00:00"                                |
| instrument_id       | `str` or `list[str]`        | `None`  | "rpg-fmcw-94"                                        |
| instrument_pid      | `str` or `list[str]`        | `None`  | "https://hdl.handle.net/21.12132/3.191564170f8a4686" |
| model_id            | `str` or `list[str]`        | `None`  | "gdas1"                                              |
| product_id\*        | `str` or `list[str]`        | `None`  | "classification"                                     |
| show_legacy\*       | `bool`                      | `False` |                                                      |
| filename_prefix\*\* | `str` or `list[str]`        | `None`  | "stare"                                              |
| filename_suffix\*\* | `str` or `list[str]`        | `None`  | ".lv1"                                               |
| status\*\*          | `str` or `list[str]`        | `None`  | "created", "uploaded", "processed" or "invalid"      |

\* = only in `files()`

\*\* = only in `raw_files()`

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

| name                 | type                                           | default |
| -------------------- | ---------------------------------------------- | ------- |
| metadata             | `list[RawMetadata]` or `list[ProductMetadata]` |         |
| include_pattern      | `str`                                          | `None`  |
| exclude_pattern      | `str`                                          | `None`  |
| include_tag_subset\* | `set[str]`                                     | `None`  |
| exclude_tag_subset\* | `set[str]`                                     | `None`  |

\* = only with `RawMetadata`

### `APIClient().file()` &rarr; `ProductMetadata`

Fetch metadata of a single file.

Parameters:

| name | type                 |
| ---- | -------------------- |
| uuid | `str` or `uuid.UUID` |

### `APIClient().sites()` &rarr; `list[Site]`

Fetch all sites.

Parameters:

| name | type  | choices                                   | default |
| ---- | ----- | ----------------------------------------- | ------- |
| type | `str` | "cloudnet", "campaign", "model", "hidden" | `None`  |

### `APIClient().site()` &rarr; `Site`

Fetch a single site.

Parameters:

| name    | type  |
| ------- | ----- |
| site_id | `str` |

### `APIClient().products()` &rarr; `list[Product]`

Fetch all products.

Parameters:

| name | type                 | choices                                   | default |
| ---- | -------------------- | ----------------------------------------- | ------- |
| type | `str` or `list[str]` | "instrument", "geophysical", "evaluation" | `None`  |

### `APIClient().product()` &rarr; `ExtendedProduct`

Fetch a single product.

Parameters:

| name       | type  |
| ---------- | ----- |
| product_id | `str` |

### `APIClient().instruments()` &rarr; `list[Instrument]`

Fetch all instruments.

### `APIClient().instrument()` &rarr; `ExtendedInstrument`

Fetch a single instruments.

Parameters:

| name | type  |
| ---- | ----- |
| uuid | `str` |

### `APIClient().models()` &rarr; `list[Model]`

Fetch all models.

### `APIClient().instrument()` &rarr; `Model`

Fetch a single model.

Parameters:

| name     | type  |
| -------- | ----- |
| model_id | `str` |

### `APIClient().versions()` &rarr; `list[VersionMetadata]`

Fetch information of all versions of a file.

Parameters:

| name | type                 |
| ---- | -------------------- |
| uuid | `str` or `uuid.UUID` |

### `APIClient().download(list[Metadata])` &rarr; `list[Path]`

Download files from the fetched metadata.

Parameters:

| name              | type                                           | default           |
| ----------------- | ---------------------------------------------- | ----------------- |
| metadata          | `list[RawMetadata]` or `list[ProductMetadata]` |                   |
| output_directory  | `PathLike` or `str`                            | Current directory |
| concurrency_limit | `int`                                          | 5                 |
| progress          | `bool` or `None`                               | `None`            |
| validate_checksum | `bool`                                         | `False`           |

There's also an asynchronous version of this function:
`cloudnet_api_client.adownload`. It's useful for usage inside Jupyter notebook.

## License

MIT
