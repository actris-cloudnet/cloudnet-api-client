[![CI](https://github.com/actris-cloudnet/cloudnet-api-client/actions/workflows/test.yml/badge.svg)](https://github.com/actris-cloudnet/cloudnet-api-client/actions/workflows/test.yml)

# cloudnet-api-client

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
cac.download(metadata, "data/")

raw_metadata = client.raw_metadata("granada", date="2024-01", instrument_id="parsivel")
cac.download(raw_metadata, "data_raw/")
```

## Documentation

### `APIClient().metadata()` and `raw_metadata()` &rarr; `[Metadata]`

Fetch product and raw file metadata from the Cloudnet data portal.

Parameters:

| name            | type                     | default | example                                              |
| --------------- | ------------------------ | ------- | ---------------------------------------------------- |
| site_id         | `str`                    |         | "hyytiala"                                           |
| date            | `str` or `datetime.date` | `None`  | "2024-01-01"                                         |
| date_from       | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| date_to         | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| updated_at_from | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| updated_at_to   | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| instrument_id   | `str` or `[str]`         | `None`  | "rpg-fmcw-94"                                        |
| instrument_pid  | `str` or `[str]`         | `None`  | "https://hdl.handle.net/21.12132/3.191564170f8a4686" |
| product\*       | `str` or `[str]`         | `None`  | "classification"                                     |
| show_legacy\*   | `bool`                   | `False` |                                                      |

\* = only in `metadata()`

**Date Handling**

The `date` parameter supports:

- "YYYY-MM-DD" — a specific date
- "YYYY-MM" — the entire month
- "YYYY" — the entire year
- Or directly as `datetime.date` object

The `date_from`, `date_to`, `updated_at_from` and `updated_at_to` parameters
should be of form "YYYY-MM-DD" or `datetime.date`. Note that, if `date` is defined, `date_from` and `date_to` have no effect.

**Return value**

Both methods return a list of `dataclass` instances, `ProductMetadata` and `RawMetadata`, respectively.

### `APIClient().filter([Metadata])` &rarr; `[Metadata]`

Additional filtering of fetched metadata.

Parameters:

| name               | type                                   | default |
| ------------------ | -------------------------------------- | ------- |
| metadata           | `[RawMetadata]` or `[ProductMetadata]` |         |
| include_pattern    | `str`                                  | `None`  |
| exclude_pattern    | `str`                                  | `None`  |
| filename_prefix    | `str`                                  | `None`  |
| filename_suffix    | `str`                                  | `None`  |
| include_tag_subset | `{str}`                                | `None`  |
| exclude_tag_subset | `{str}`                                | `None`  |

### `APIClient().sites()` &rarr; `[Site]`

Fetch cloudnet sites.

Parameters:

| name | type             | Choices                                   | default |
| ---- | ---------------- | ----------------------------------------- | ------- |
| type | `str` or `[str]` | "cloudnet", "campaign", "model", "hidden" | `None`  |

### `APIClient().products()` &rarr; `[Product]`

Fetch cloudnet products.

Parameters:

| name | type             | Choices                                   | default |
| ---- | ---------------- | ----------------------------------------- | ------- |
| type | `str` or `[str]` | "instrument", "geophysical", "evaluation" | `None`  |

### `cloudnet_api_client.download([Metadata])`

Download files from the fetched metadata.

Parameters:

| name              | type                                   | default |
| ----------------- | -------------------------------------- | ------- |
| metadata          | `[RawMetadata]` or `[ProductMetadata]` |         |
| output_directory  | `PathLike` or `str`                    |         |
| concurrency_limit | `int`                                  | 5       |

## License

MIT
