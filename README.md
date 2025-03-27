# cloudnet-api-client

Official Python package for using Cloudnet API

## Quickstart

```python
import cloudnet_api_client as cac

client = cac.APIClient()

sites = client.sites(type="cloudnet")
products = client.products()

metadata = client.metadata("hyytiala", "2021-01-01", product=["mwr", "radar"])
cac.download(metadata, "data")

raw_metadata = client.metadata("hyytiala", "2021-01-01")
cac.download(raw_metadata, "data_raw")
```

## Documentation

### `APIClient().metadata()` and `raw_metadata()` &rarr; `[Metadata]`

Fetch product and raw file metadata from the Cloudnet data portal.

Parameters:

| name           | type                     | default | example                                              |
| -------------- | ------------------------ | ------- | ---------------------------------------------------- |
| site_id        | `str`                    |         | "hyytiala"                                           |
| date           | `str` or `datetime.date` | `None`  | "2024-01-01"                                         |
| date_from      | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| date_to        | `str` or `datetime.date` | `None`  | "2025-01-01"                                         |
| instrument_id  | `str` or `list[str]`     | `None`  | "rpg-fmcw-94"                                        |
| instrument_pid | `str` or `list[str]`     | `None`  | "https://hdl.handle.net/21.12132/3.191564170f8a4686" |
| product\*      | `str` or `list[str]`     | `None`  | "classification"                                     |
| show_legacy\*  | `bool`                   | `False` |                                                      |

\* = only in `metadata()`

**Date Handling**

The `date` parameter supports:

- "YYYY-MM-DD" — a specific date
- "YYYY-MM" — the entire month
- "YYYY" — the entire year
- Or directly as `datetime.date` object

The `date_from` and `date_to` parameters should be "YYYY-MM-DD" or `datetime.date`. If `date` is defined, `date_from` and `date_to` have no effect.

**Return value**

Both methods return a list of `dataclass` instances, `ProductMetadata` and `RawMetadata`, respectively.

### `APIClient().filter([Metadata])` &rarr; `[Metadata]`

Additional filtering of fetched metadata.

Parameters:

| name               | type                                 | default |
| ------------------ | ------------------------------------ | ------- |
| metadata           | `list[RawMetadata\|ProductMetadata]` |         |
| include_pattern    | `str`                                | `None`  |
| exclude_pattern    | `str`                                | `None`  |
| filename_prefix    | `str`                                | `None`  |
| filename_suffix    | `str`                                | `None`  |
| include_tag_subset | `set[str]`                           | `None`  |
| exclude_tag_subset | `set[str]`                           | `None`  |

### `cloudnet_api_client.download([Metadata])`

Function to download files from fetched metadata.

Parameters:

| name              | type                                 | default |
| ----------------- | ------------------------------------ | ------- |
| metadata          | `list[RawMetadata\|ProductMetadata]` |         |
| output_directory  | `PathLike`                           |         |
| concurrency_limit | `int`                                | 5       |

## License

MIT
