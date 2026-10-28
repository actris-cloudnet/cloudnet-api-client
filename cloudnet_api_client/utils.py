"""Utility functions for the Cloudnet API client.

This module provides exception classes and hash calculation utilities.
"""

import base64
import datetime
import hashlib
import re
from collections.abc import Iterable
from dataclasses import fields, is_dataclass
from os import PathLike
from platform import platform
from typing import Literal, TypeAlias, TypeVar, cast, get_args
from uuid import UUID

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cloudnet_api_client.containers import (
    Instrument,
    Metadata,
    Model,
    Product,
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
    Site,
)

from .version import __version__

T = TypeVar("T")
MetadataList = (
    Iterable[ProductMetadata] | Iterable[RawMetadata] | Iterable[RawModelMetadata]
)
TMetadata = TypeVar("TMetadata", ProductMetadata, RawMetadata, RawModelMetadata)
DateParam = str | datetime.date | None
DateTimeParam = str | datetime.datetime | datetime.date | None
QueryParam: TypeAlias = T | Iterable[T] | None


class CloudnetAPIError(Exception):
    """Exception raised for errors from the Cloudnet API.

    Attributes:
        message: The error message from the API.
    """

    def __init__(self, msg: str):
        """Initialize the CloudnetAPIError.

        Args:
            msg: The error message from the API.
        """
        self.message = msg
        super().__init__(self.message)


class InvalidChecksumError(Exception):
    """Exception raised if downloaded file has invalid checksum.

    Attributes:
        checksum: Checksum of downloaded file.
        metadata: File metadata.
        message: The error message.
    """

    def __init__(self, checksum: str, metadata: Metadata):
        """Initialize the InvalidChecksumError.

        Args:
            checksum: Checksum of downloaded file.
            metadata: File metadata.
        """
        self.checksum = checksum
        self.metadata = metadata
        self.message = f"Expected checksum {metadata.checksum}, got {checksum}"
        super().__init__(self.message)


def sha256sum(filename: str | PathLike) -> str:
    """Calculate the SHA256 hash of a file.

    Args:
        filename: Path to the file to hash.

    Returns:
        Hexadecimal SHA256 hash string.
    """
    return _calc_hash_sum(filename, "sha256")


def md5sum(filename: str | PathLike, is_base64: bool = False) -> str:
    """Calculate the MD5 hash of a file.

    Args:
        filename: Path to the file to hash.
        is_base64: If True, return the hash as base64. If False (default),
            return as hexadecimal.

    Returns:
        MD5 hash string, either hexadecimal or base64 encoded.
    """
    return _calc_hash_sum(filename, "md5", is_base64)


def _calc_hash_sum(
    filename: str | PathLike, method: Literal["sha256", "md5"], is_base64: bool = False
) -> str:
    """Calculate a hash sum for a file using the specified method.

    Args:
        filename: Path to the file to hash.
        method: Hash algorithm to use. Must be "sha256" or "md5".
        is_base64: If True, return the hash as base64. If False (default),
            return as hexadecimal.

    Returns:
        Hash string, either hexadecimal or base64 encoded.
    """
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.b64encode(hash_sum.digest()).decode("utf-8")
    return hash_sum.hexdigest()


CONVERTED = {
    "measurement_date",
    "created_at",
    "updated_at",
    "size",
    "uuid",
    "start_time",
    "stop_time",
}


def _build_meta_objects(
    res: list[dict], instrument_meta: dict | None = None
) -> list[ProductMetadata]:
    """Build ProductMetadata objects from API response data.

    Args:
        res: List of dictionaries containing product file metadata from the API.
        instrument_meta: Optional dictionary containing instrument metadata to use
            instead of the instrument metadata in the response.

    Returns:
        List of ProductMetadata objects constructed from the response data.
    """
    field_names = (
        {f.name for f in fields(ProductMetadata)}
        - CONVERTED
        - {"product", "instrument", "model", "site", "software"}
    )
    return [
        ProductMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            product=_build_object(obj["product"], Product),
            instrument=_create_instrument_object(instrument_meta or obj["instrument"])
            if instrument_meta or "instrument" in obj and obj["instrument"]
            else None,
            model=_create_model_object(obj["model"])
            if "model" in obj and obj["model"]
            else None,
            measurement_date=datetime.date.fromisoformat(obj["measurementDate"]),
            created_at=_parse_datetime(obj["createdAt"]),
            updated_at=_parse_datetime(obj["updatedAt"]),
            start_time=_parse_datetime(obj["startTime"]) if obj["startTime"] else None,
            stop_time=_parse_datetime(obj["stopTime"]) if obj["stopTime"] else None,
            size=int(obj["size"]),
            uuid=UUID(obj["uuid"]),
            site=_build_object(obj["site"], Site),
        )
        for obj in res
    ]


def _build_raw_meta_objects(res: list[dict]) -> list[RawMetadata]:
    """Build RawMetadata objects from API response data.

    Args:
        res: List of dictionaries containing raw file metadata from the API.

    Returns:
        List of RawMetadata objects constructed from the response data.
    """
    field_names = (
        {f.name for f in fields(RawMetadata)}
        - CONVERTED
        - {"instrument", "site", "tags"}
    )
    return [
        RawMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            instrument=_create_instrument_object(obj["instrument"]),
            measurement_date=datetime.date.fromisoformat(obj["measurementDate"]),
            created_at=_parse_datetime(obj["createdAt"]),
            updated_at=_parse_datetime(obj["updatedAt"]),
            size=int(obj["size"]),
            uuid=UUID(obj["uuid"]),
            site=_build_object(obj["site"], Site),
            tags=frozenset(obj["tags"]),
        )
        for obj in res
    ]


def _build_raw_model_meta_objects(res: list[dict]) -> list[RawModelMetadata]:
    """Build RawModelMetadata objects from API response data.

    Args:
        res: List of dictionaries containing raw model file metadata from the API.

    Returns:
        List of RawModelMetadata objects constructed from the response data.
    """
    field_names = (
        {f.name for f in fields(RawModelMetadata)} - CONVERTED - {"model", "site"}
    )
    return [
        RawModelMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            model=_create_model_object(obj["model"]),
            measurement_date=datetime.date.fromisoformat(obj["measurementDate"]),
            created_at=_parse_datetime(obj["createdAt"]),
            updated_at=_parse_datetime(obj["updatedAt"]),
            size=int(obj["size"]),
            uuid=UUID(obj["uuid"]),
            site=_build_object(obj["site"], Site),
        )
        for obj in res
    ]


def _create_model_object(meta: dict) -> Model:
    """Create a Model object from API response data.

    Args:
        meta: Dictionary containing model metadata from the API.

    Returns:
        Model object constructed from the metadata.
    """
    return Model(
        id=meta["id"],
        name=meta["humanReadableName"],
        optimum_order=int(meta["optimumOrder"]),
        source_model_id=meta["sourceModelId"],
        forecast_start=int(meta["forecastStart"])
        if meta["forecastStart"] is not None
        else None,
        forecast_end=int(meta["forecastEnd"])
        if meta["forecastEnd"] is not None
        else None,
    )


def _create_instrument_object(meta: dict) -> Instrument:
    """Create an Instrument object from API response data.

    Args:
        meta: Dictionary containing instrument metadata from the API.

    Returns:
        Instrument object constructed from the metadata.
    """
    return Instrument(
        instrument_id=meta.get("instrument", {}).get("id") or meta["instrumentId"],
        model=meta["model"],
        type=meta["type"],
        uuid=UUID(meta["uuid"]),
        pid=meta["pid"],
        owners=tuple(meta["owners"]),
        serial_number=meta["serialNumber"],
        name=meta["name"],
    )


def _build_objects(data_list: list[dict], cls: type[T]) -> list[T]:
    """Build a list of dataclass objects from API response data.

    Args:
        data_list: List of dictionaries containing data from the API.
        cls: The dataclass type to construct objects from.

    Returns:
        List of dataclass objects constructed from the data.
    """
    return [_build_object(d, cls) for d in data_list]


def _build_object(data: dict, cls: type[T]) -> T:
    """Build a dataclass object from API response data.

    This function converts camelCase keys to snake_case and handles list fields
    by converting them to frozensets.

    Args:
        data: Dictionary containing data from the API.
        cls: The dataclass type to construct an object from.

    Returns:
        Dataclass object constructed from the data.
    """
    assert is_dataclass(cls)
    field_names = {f.name for f in fields(cls)}
    kwargs = {}
    for k, v in data.items():
        snake_key = _to_snake(k)
        if snake_key in field_names:
            if isinstance(v, list):
                kwargs[snake_key] = frozenset(v)
            else:
                kwargs[snake_key] = v
    object = cls(**kwargs)
    return cast(T, object)


def _to_snake(name: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case.

    Args:
        name: String to convert.

    Returns:
        String in snake_case format.
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _set_of_ids(res: dict, name: str) -> frozenset[str]:
    """Extract a frozenset of IDs from a nested list in a dictionary.

    Args:
        res: Dictionary containing a list of objects with 'id' fields.
        name: Key in the dictionary where the list of objects is stored.

    Returns:
        Frozenset of ID strings from the list of objects.
    """
    return frozenset(obj["id"] for obj in res.get(name, []))


def _make_session() -> requests.Session:
    """Create a requests session configured for the Cloudnet API.

    The session includes a custom User-Agent header and retry strategy.

    Returns:
        Configured requests.Session object.
    """
    session = requests.Session()
    session.headers.update(
        {"User-Agent": f"cloudnet-api-client/{__version__} ({platform()})"}
    )
    retry_strategy = Retry(total=10, backoff_factor=0.1, status_forcelist=[524])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _parse_datetime(dt: str) -> datetime.datetime:
    """Parse a datetime string from the API into a datetime object.

    Supports datetime strings with and without microseconds.

    Args:
        dt: Datetime string to parse.

    Returns:
        datetime.datetime object with UTC timezone.

    Raises:
        ValueError: If the datetime format is not recognized.
    """
    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=datetime.timezone.utc
        )
    except ValueError:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )


def _check_params(params: dict, ignore: tuple = ()) -> None:
    """Check that at least one parameter is set.

    Args:
        params: Dictionary of parameters to check.
        ignore: Tuple of parameter names to ignore in the check.

    Raises:
        TypeError: If no parameters (excluding ignored ones) are set.
    """
    if sum(1 for key, value in params.items() if key not in ignore and value) == 0:
        raise TypeError("At least one of the parameters must be set.")


def _asdict_shallow(obj) -> dict:
    """Convert a dataclass object to a shallow dictionary.

    Args:
        obj: Dataclass object to convert.

    Returns:
        Dictionary mapping field names to their values.
    """
    return dict((field.name, getattr(obj, field.name)) for field in fields(obj))


def _validate_type(values, literal) -> list | None:
    """Validate that values are within the allowed literal type values.

    Args:
        values: Value or list of values to validate. Can be a single string or
            a list of strings.
        literal: A typing.Literal type containing the allowed values.

    Returns:
        List of validated values, or None if values is None.

    Raises:
        ValueError: If any value is not in the allowed values.
    """
    if values is None:
        return None
    if isinstance(values, str):
        values = [values]
    allowed_values = get_args(literal)
    output = []
    for value in values:
        if value not in allowed_values:
            raise ValueError(f"Invalid type: {value}")
        output.append(value)
    return output
