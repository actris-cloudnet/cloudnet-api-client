"""Data container classes for the Cloudnet API client.

This module defines all the dataclass types used to represent data returned
by the Cloudnet API, including sites, products, instruments, models, metadata,
and version information.
"""

import datetime
import uuid
from dataclasses import dataclass
from typing import Literal

SITE_TYPE = Literal[
    "cloudnet",
    "model",
    "hidden",
    "campaign",
    "mobile",
    "arm",
    "weather-radar",
    "fmi-radar",
]
"""Literal type for site types in the Cloudnet API."""

PRODUCT_TYPE = Literal["instrument", "geophysical", "evaluation", "model"]
"""Literal type for product types in the Cloudnet API."""

STATUS = Literal["created", "uploaded", "processed", "invalid"]
"""Literal type for file status values in the Cloudnet API."""

TIMELINESS = Literal["rrt", "nrt", "scheduled"]
"""Literal type for timeliness values in the Cloudnet API.

Values:
    rrt: Real-time or near real-time
    nrt: Near real-time
    scheduled: Scheduled processing
"""


@dataclass(frozen=True, slots=True)
class MeanLocation:
    """Represents a mean location for a moving site on a specific date.

    Attributes:
        time: The date for which the location is valid.
        latitude: The latitude coordinate in decimal degrees.
        longitude: The longitude coordinate in decimal degrees.
    """

    time: datetime.date
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class RawLocation:
    """Represents a raw location point for a moving site.

    Attributes:
        time: The timestamp for which the location is valid.
        latitude: The latitude coordinate in decimal degrees.
        longitude: The longitude coordinate in decimal degrees.
    """

    time: datetime.datetime
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class Site:
    """Represents a measurement site in the Cloudnet network.

    Attributes:
        id: The unique identifier for the site.
        human_readable_name: The human-readable name of the site.
        station_name: The name of the station, if available.
        latitude: The latitude coordinate in decimal degrees.
        longitude: The longitude coordinate in decimal degrees.
        altitude: The altitude above sea level in meters.
        dvas_id: The DVAS identifier for the site, if available.
        actris_id: The ACTRIS identifier for the site, if available.
        country: The country where the site is located.
        country_code: The ISO 3166-1 alpha-2 country code.
        country_subdivision_code: The country subdivision code, if available.
        type: Set of site type classifications.
        gaw: The GAW identifier, if available.
    """

    id: str
    human_readable_name: str
    station_name: str | None
    latitude: float | None
    longitude: float | None
    altitude: int
    dvas_id: str | None
    actris_id: int | None
    country: str
    country_code: str
    country_subdivision_code: str | None
    type: frozenset[SITE_TYPE]
    gaw: str | None


@dataclass(frozen=True, slots=True)
class Product:
    """Represents a product in the Cloudnet data portal.

    Attributes:
        id: The unique identifier for the product.
        human_readable_name: The human-readable name of the product.
        type: Set of product type classifications.
        experimental: Whether the product is experimental.
    """

    id: str
    human_readable_name: str
    type: frozenset[PRODUCT_TYPE]
    experimental: bool


@dataclass(frozen=True, slots=True)
class ExtendedProduct(Product):
    """Extended product information with source and derived product IDs.

    Attributes:
        source_instrument_ids: Set of instrument IDs that are sources for this product.
        source_product_ids: Set of product IDs that are sources for this product.
        derived_product_ids: Set of product IDs that are derived from this product.
    """

    source_instrument_ids: frozenset[str]
    source_product_ids: frozenset[str]
    derived_product_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class Instrument:
    """Represents an instrument in the Cloudnet data portal.

    Attributes:
        instrument_id: The unique identifier for the instrument.
        model: The instrument model from ACTRIS Vocabulary (e.g., "RPG-FMCW-94 DP").
        type: The instrument type from ACTRIS Vocabulary
            (e.g., "Doppler non-scanning cloud radar").
        name: The human-readable name of the instrument
            (e.g., "FMI RPG-FMCW-94 (Pallas)").
        uuid: The UUID identifier for the instrument.
        pid: The persistent identifier (PID) for the instrument.
        owners: Tuple of owner identifiers.
        serial_number: The serial number of the instrument, if available.
    """

    instrument_id: str
    model: str  # From ACTRIS Vocabulary, e.g. "RPG-FMCW-94 DP"
    type: str  # From ACTRIS Vocabulary, e.g. "Doppler non-scanning cloud radar"
    name: str  # e.g. "FMI RPG-FMCW-94 (Pallas)"
    uuid: uuid.UUID
    pid: str
    owners: tuple[str, ...]  # could be ordered
    serial_number: str | None


@dataclass(frozen=True, slots=True)
class ExtendedInstrument(Instrument):
    """Extended instrument information with derived product IDs.

    Attributes:
        derived_product_ids: Set of product IDs that are derived from this instrument.
    """

    derived_product_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class Model:
    """Represents a numerical weather prediction model in the Cloudnet data portal.

    Attributes:
        id: The unique identifier for the model.
        name: The human-readable name of the model.
        optimum_order: The optimum order for this model.
        source_model_id: The identifier of the source model.
        forecast_start: The forecast start step as hours.
        forecast_end: The forecast end step as hours.
    """

    id: str
    name: str
    optimum_order: int
    source_model_id: str
    forecast_start: int | None
    forecast_end: int | None


@dataclass(frozen=True, slots=True)
class Software:
    """Represents software used in data processing.

    Attributes:
        id: The unique identifier for the software.
        version: The version of the software.
        title: The title of the software.
        url: The URL where the software can be found.
    """

    id: str
    version: str
    title: str
    url: str


@dataclass(frozen=True, slots=True)
class Metadata:
    """Base class for file metadata in the Cloudnet data portal.

    Attributes:
        uuid: The UUID identifier for the file.
        checksum: The checksum of the file.
        size: The size of the file in bytes.
        filename: The name of the file.
        download_url: The URL to download the file.
        measurement_date: The date when the measurement was taken.
        created_at: The timestamp when the file was created.
        updated_at: The timestamp when the file was last updated.
        site: The site where the measurement was taken.
    """

    uuid: uuid.UUID
    checksum: str
    size: int
    filename: str
    download_url: str
    measurement_date: datetime.date
    created_at: datetime.datetime
    updated_at: datetime.datetime
    site: Site


@dataclass(frozen=True, slots=True)
class RawMetadata(Metadata):
    """Metadata for raw data files.

    Attributes:
        status: The processing status of the file.
        instrument: The instrument that generated the data.
        tags: Set of tags associated with the file.
    """

    status: STATUS
    instrument: Instrument
    tags: frozenset[str]


@dataclass(frozen=True, slots=True)
class RawModelMetadata(Metadata):
    """Metadata for raw model data files.

    Attributes:
        status: The processing status of the file.
        model: The model that generated the data.
    """

    status: STATUS
    model: Model


@dataclass(frozen=True, slots=True)
class ProductMetadata(Metadata):
    """Metadata for processed product files.

    Attributes:
        product: The product type.
        instrument: The instrument that generated the data, if available.
        model: The model used for the data, if available.
        volatile: Whether the file is volatile (may be regenerated).
        legacy: Whether the file is from a legacy processing chain.
        pid: The persistent identifier for the file.
        dvas_id: The DVAS identifier for the file, if available.
        error_level: The error level of the file, if applicable.
        coverage: The data coverage as a percentage.
        timeliness: The timeliness classification of the file.
        format: The file format.
        start_time: The start time of the measurement period.
        stop_time: The end time of the measurement period.
        s3key: The S3 object key, if available.
    """

    product: Product
    instrument: Instrument | None
    model: Model | None
    volatile: bool
    legacy: bool
    pid: str
    dvas_id: str | None
    error_level: str | None
    coverage: float
    timeliness: TIMELINESS
    format: str
    start_time: datetime.datetime | None
    stop_time: datetime.datetime | None
    s3key: str | None


@dataclass(frozen=True, slots=True)
class ExtendedProductMetadata(ProductMetadata):
    """Extended product metadata with software information.

    Attributes:
        software: Tuple of software packages used to process the data.
    """

    software: tuple[Software, ...]


@dataclass(frozen=True, slots=True)
class VersionMetadata:
    """Metadata for a specific version of a file.

    Attributes:
        uuid: The UUID identifier for the version.
        created_at: The timestamp when the version was created.
        pid: The persistent identifier for the version.
        checksum: The checksum of the version.
        legacy: Whether the version is from a legacy processing chain.
        size: The size of the version in bytes.
        dvas_id: The DVAS identifier for the version, if available.
    """

    uuid: uuid.UUID
    created_at: datetime.datetime
    pid: str
    checksum: str
    legacy: bool
    size: int
    dvas_id: str | None
