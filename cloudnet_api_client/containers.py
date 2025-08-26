import datetime
import uuid
from dataclasses import dataclass
from typing import Literal

SITE_TYPE = Literal["cloudnet", "model", "hidden", "campaign"]
PRODUCT_TYPE = Literal["instrument", "geophysical", "evaluation", "model"]
STATUS = Literal["created", "uploaded", "processed", "invalid"]


@dataclass(frozen=True, slots=True)
class Location:
    time: datetime.datetime | datetime.date
    latitude: float
    longitude: float


@dataclass(frozen=True, slots=True)
class Site:
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
    id: str
    human_readable_name: str
    type: frozenset[PRODUCT_TYPE]
    experimental: bool


@dataclass(frozen=True, slots=True)
class ExtendedProduct(Product):
    source_instrument_ids: frozenset[str]
    source_product_ids: frozenset[str]
    derived_product_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class Instrument:
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
    derived_product_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class Model:
    id: str
    name: str
    optimum_order: int
    source_model_id: str
    forecast_start: int | None
    forecast_end: int | None


@dataclass(frozen=True, slots=True)
class Metadata:
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
    status: STATUS
    instrument: Instrument
    tags: frozenset[str]


@dataclass(frozen=True, slots=True)
class RawModelMetadata(Metadata):
    status: STATUS
    model: Model


@dataclass(frozen=True, slots=True)
class ProductMetadata(Metadata):
    product: Product
    instrument: Instrument | None
    model: Model | None
    volatile: bool
    legacy: bool
    pid: str
    dvas_id: str | None
    error_level: str | None
    coverage: float
    timeliness: str
    format: str
    start_time: datetime.datetime | None
    stop_time: datetime.datetime | None


@dataclass(frozen=True, slots=True)
class VersionMetadata:
    uuid: uuid.UUID
    created_at: datetime.datetime
    pid: str
    checksum: str
    legacy: bool
    size: int
    dvas_id: str | None
