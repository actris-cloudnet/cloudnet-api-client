import uuid
from dataclasses import dataclass
from typing import Literal

SITE_TYPE = Literal["cloudnet", "model", "hidden", "campaign"]
PRODUCT_TYPE = Literal["instrument", "geophysical", "evaluation"]


@dataclass(frozen=True, slots=True)
class Site:
    id: str
    human_readable_name: str
    station_name: str | None
    latitude: float
    longitude: float
    altitude: float
    dvas_id: str | None
    actris_id: int | None
    country: str
    country_code: str
    country_subdivision_code: str | None
    type: SITE_TYPE
    status: Literal["active", "inactive"]
    gaw: str | None


@dataclass(frozen=True, slots=True)
class Product:
    id: str
    human_readable_name: str
    type: PRODUCT_TYPE
    experimental: bool


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_id: str  # CLU internal identifier, e.g. "rpg-fmcw-94"
    model: str  # From ACTRIS Vocabulary, e.g. "RPG-FMCW-94 DP"
    type: str  # From ACTRIS Vocabulary, e.g. "Doppler non-scanning cloud radar"
    name: str  # e.g. "FMI RPG-FMCW-94 (Pallas)"
    uuid: uuid.UUID
    pid: str
    owners: list[str]
    serial_number: str | None


@dataclass(frozen=True, slots=True)
class Metadata:
    uuid: uuid.UUID
    checksum: str
    size: int
    filename: str
    measurement_date: str
    download_url: str
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class RawMetadata(Metadata):
    status: Literal["created", "uploaded", "processed", "invalid"]
    instrument: Instrument
    tags: list[str] | None


@dataclass(frozen=True, slots=True)
class ProductMetadata(Metadata):
    product: Product
