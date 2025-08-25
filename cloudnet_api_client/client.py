import asyncio
import calendar
import datetime
import os
import re
import warnings
from dataclasses import asdict, fields, is_dataclass
from os import PathLike
from pathlib import Path
from platform import platform
from typing import TypeVar, cast, get_args
from urllib.parse import urljoin
from uuid import UUID

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cloudnet_api_client.containers import (
    PRODUCT_TYPE,
    SITE_TYPE,
    STATUS,
    ExtendedInstrument,
    ExtendedProduct,
    Instrument,
    Location,
    Model,
    Product,
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
    Site,
    VersionMetadata,
)
from cloudnet_api_client.dl import download_files

from .utils import CloudnetAPIError
from .version import __version__

T = TypeVar("T")
MetadataList = list[ProductMetadata] | list[RawMetadata] | list[RawModelMetadata]
TMetadata = TypeVar("TMetadata", ProductMetadata, RawMetadata, RawModelMetadata)
DateParam = str | datetime.date | None
DateTimeParam = str | datetime.datetime | datetime.date | None
QueryParam = str | list[str] | None


class APIClient:
    def __init__(
        self,
        base_url: str = "https://cloudnet.fmi.fi/api/",
        session: requests.Session | None = None,
    ) -> None:
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.session = session or _make_session()

    def sites(
        self,
        type: SITE_TYPE | list[SITE_TYPE] | None = None,
    ) -> list[Site]:
        type = validate_type(type, SITE_TYPE)
        res = self._get("sites", {"type": type})
        return _build_objects(res, Site)

    def site(self, site_id: str) -> Site:
        res = self._get(f"sites/{site_id}")[0]
        return _build_object(res, Site)

    def products(
        self, type: PRODUCT_TYPE | list[PRODUCT_TYPE] | None = None
    ) -> list[Product]:
        type = validate_type(type, PRODUCT_TYPE)
        data = self._get("products")
        if type is not None:
            data = [obj for obj in data if any(t in obj["type"] for t in type)]
        return _build_objects(data, Product)

    def product(self, product_id: str) -> ExtendedProduct:
        res = self._get(f"products/{product_id}")[0]
        obj = _build_object(res, Product)
        return ExtendedProduct(
            **asdict(obj),
            derived_product_ids=_set_of_ids(res, "derivedProducts"),
            source_instrument_ids=_set_of_ids(res, "sourceInstruments"),
            source_product_ids=_set_of_ids(res, "sourceProducts"),
        )

    def instruments(self) -> list[Instrument]:
        res = self._get("instrument-pids")
        return [_create_instrument_object(obj) for obj in res]

    def instrument(self, uuid: str | UUID) -> ExtendedInstrument:
        res = self._get(f"instrument-pids/{uuid}")[0]
        obj = _create_instrument_object(res)
        return ExtendedInstrument(
            **asdict(obj),
            derived_product_ids=self.instrument_derived_products(obj.instrument_id),
        )

    def instrument_derived_products(self, instrument_id: str) -> frozenset[str]:
        res = self._get(f"instruments/{instrument_id}")[0]
        return _set_of_ids(res, "derivedProducts")

    def instrument_ids(self) -> frozenset[str]:
        res = self._get("instruments")
        return frozenset(obj["id"] for obj in res)

    def models(self) -> list[Model]:
        res = self._get("models")
        return [_create_model_object(obj) for obj in res]

    def model(self, model_id: str) -> Model:
        res = self._get("models")
        model = [r for r in res if r["id"] == model_id]
        if not model:
            raise CloudnetAPIError(f"Model with id {model_id} not found")
        return _create_model_object(model[0])

    def file(
        self,
        uuid: str | UUID,
    ) -> ProductMetadata:
        file_res = self._get(f"files/{uuid}")[0]
        if file_res.get("instrument") is not None:
            instrument_uuid = file_res["instrument"]["uuid"]
            instrument_res = self._get(f"instrument-pids/{instrument_uuid}")[0]
        else:
            instrument_res = None
        return _build_meta_objects([file_res], instrument_res)[0]

    def versions(self, uuid: str | UUID) -> list[VersionMetadata]:
        payload = {"properties": ["pid", "dvasId", "legacy", "size", "checksum"]}
        res = self._get(f"files/{uuid}/versions", params=payload)
        return [
            VersionMetadata(
                uuid=UUID(obj["uuid"]),
                created_at=_parse_datetime(obj["createdAt"]),
                pid=obj["pid"],
                dvas_id=obj["dvasId"],
                legacy=obj["legacy"],
                size=int(obj["size"]),
                checksum=obj["checksum"],
            )
            for obj in res
        ]

    def files(
        self,
        site_id: QueryParam = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        instrument_id: QueryParam = None,
        instrument_pid: QueryParam = None,
        model_id: QueryParam = None,
        product_id: QueryParam = None,
        show_legacy: bool = False,
    ) -> list[ProductMetadata]:
        params = {
            "site": site_id,
            "instrument": instrument_id,
            "instrumentPid": instrument_pid,
            "product": product_id,
            "showLegacy": show_legacy,
        }
        if show_legacy is not True:
            # API shows legacy files with any value (even <False>)
            del params["showLegacy"]

        _add_date_params(
            params, date, date_from, date_to, updated_at, updated_at_from, updated_at_to
        )

        _check_params({**params, "model": model_id}, ("showLegacy",))

        no_instrument = instrument_id is None and instrument_pid is None

        if no_instrument and (product_id is None and model_id is not None):
            files_res = []
        else:
            files_res = self._get("files", params, expected_code=400)

        # Add model files if requested
        if (
            (product_id is None and no_instrument)
            or (product_id is not None and "model" in product_id)
            or (model_id is not None and (product_id is None or "model" in product_id))
        ):
            for key in ("showLegacy", "product", "instrument", "instrumentPid"):
                if key in params:
                    del params[key]
            params["model"] = model_id
            files_res += self._get("model-files", params, expected_code=400)

        return _build_meta_objects(files_res)

    def metadata(self, *args, **kwargs):
        warnings.warn("use files instead of metadata", DeprecationWarning, stacklevel=2)
        return self.files(*args, **kwargs)

    def raw_files(
        self,
        site_id: QueryParam = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        instrument_id: QueryParam = None,
        instrument_pid: QueryParam = None,
        filename_prefix: QueryParam = None,
        filename_suffix: QueryParam = None,
        status: STATUS | list[STATUS] | None = None,
    ) -> list[RawMetadata]:
        params = {
            "site": site_id,
            "instrument": instrument_id,
            "instrumentPid": instrument_pid,
            "filenamePrefix": filename_prefix,
            "filenameSuffix": filename_suffix,
            "status": status,
        }
        _add_date_params(
            params, date, date_from, date_to, updated_at, updated_at_from, updated_at_to
        )
        res = self._get("raw-files", params, expected_code=400)
        return _build_raw_meta_objects(res)

    def raw_metadata(self, *args, **kwargs):
        warnings.warn(
            "use raw_files instead of raw_metadata", DeprecationWarning, stacklevel=2
        )
        return self.raw_files(*args, **kwargs)

    def raw_model_files(
        self,
        site_id: QueryParam = None,
        model_id: QueryParam = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        filename_prefix: QueryParam = None,
        filename_suffix: QueryParam = None,
        status: STATUS | list[STATUS] | None = None,
    ) -> list[RawModelMetadata]:
        """For internal CLU use only. Will change in the future."""
        params = {
            "site": site_id,
            "filenamePrefix": filename_prefix,
            "filenameSuffix": filename_suffix,
            "status": status,
            "model": model_id,
        }
        _add_date_params(
            params, date, date_from, date_to, updated_at, updated_at_from, updated_at_to
        )

        _check_params(params)

        res = self._get("raw-model-files", params, expected_code=400)
        return _build_raw_model_meta_objects(res)

    def moving_site_mean_location(
        self, site_id: str, date: datetime.date | str
    ) -> Location:
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"date": date}
        res = self._get(f"sites/{site_id}/locations", params=payload)[0]
        return Location(
            time=date,
            latitude=res["latitude"],
            longitude=res["longitude"],
        )

    def moving_site_locations(
        self, site_id: str, date: datetime.date | str
    ) -> list[Location]:
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"date": date, "raw": "1"}
        locations = self._get(f"sites/{site_id}/locations", params=payload)
        return [
            Location(
                time=_parse_datetime(location["date"]),
                latitude=location["latitude"],
                longitude=location["longitude"],
            )
            for location in locations
        ]

    def source_instruments(self, uuid: UUID | str) -> set[ExtendedInstrument]:
        """Recursively finds source instruments of a product file."""
        instruments = set()
        res = self._get(f"files/{uuid}")[0]
        if res.get("instrument"):
            instrument = self.instrument(res["instrument"]["uuid"])
            instruments.add(instrument)
        for source_id in res.get("sourceFileIds", []):
            instruments |= self.source_instruments(source_id)
        return instruments

    def calibration(self, instrument_pid: str, date: datetime.date | str) -> dict:
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"instrumentPid": instrument_pid, "date": date.isoformat()}
        return self._get("calibration", params=payload)[0]

    def download(
        self,
        metadata: MetadataList,
        output_directory: str | PathLike = ".",
        concurrency_limit: int = 5,
        progress: bool | None = None,
        validate_checksum: bool = False,
    ) -> list[Path]:
        return asyncio.run(
            self.adownload(
                metadata,
                output_directory,
                concurrency_limit,
                progress,
                validate_checksum,
            )
        )

    async def adownload(
        self,
        metadata: MetadataList,
        output_directory: str | PathLike = ".",
        concurrency_limit: int = 5,
        progress: bool | None = None,
        validate_checksum: bool = False,
    ) -> list[Path]:
        disable_progress = not progress if progress is not None else None
        output_directory = Path(output_directory).resolve()
        os.makedirs(output_directory, exist_ok=True)
        return await download_files(
            self.base_url,
            metadata,
            output_directory,
            concurrency_limit,
            disable_progress,
            validate_checksum,
        )

    @staticmethod
    def filter(
        metadata: list[TMetadata],
        include_pattern: str | None = None,
        exclude_pattern: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
    ) -> list[TMetadata]:
        if include_pattern:
            metadata = [
                m for m in metadata if re.search(include_pattern, m.filename, re.I)
            ]
        if exclude_pattern:
            metadata = [
                m for m in metadata if not re.search(exclude_pattern, m.filename, re.I)
            ]
        if include_tag_subset:
            metadata = [
                m
                for m in metadata
                if isinstance(m, RawMetadata) and include_tag_subset.issubset(m.tags)
            ]
        if exclude_tag_subset:
            metadata = [
                m
                for m in metadata
                if isinstance(m, RawMetadata)
                and not exclude_tag_subset.issubset(m.tags)
            ]
        return metadata

    def _get(
        self, endpoint: str, params: dict | None = None, expected_code: int = 404
    ) -> list[dict]:
        try:
            url = urljoin(self.base_url, endpoint)
            res = self.session.get(url, params=params, timeout=120)
            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict):
                data = [data]
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == expected_code:
                reason = e.response.json().get("errors", "Not found")
                raise CloudnetAPIError(reason) from e
            raise


def _add_date_params(
    params: dict,
    date: DateParam,
    date_from: DateParam,
    date_to: DateParam,
    updated_at: DateTimeParam,
    updated_at_from: DateTimeParam,
    updated_at_to: DateTimeParam,
):
    if date is not None and (date_from is not None or date_to is not None):
        msg = "Cannot use 'date' with 'date_from' and 'date_to'"
        raise ValueError(msg)
    if date is not None:
        start, stop = _parse_date_param(date)
        params["dateFrom"] = start.isoformat()
        params["dateTo"] = stop.isoformat()
    if date_from is not None:
        params["dateFrom"] = _parse_date_param(date_from)[0].isoformat()
    if date_to is not None:
        params["dateTo"] = _parse_date_param(date_to)[1].isoformat()

    if updated_at is not None and (
        updated_at_from is not None or updated_at_to is not None
    ):
        msg = "Cannot use 'updated_at' with 'updated_at_from' and 'updated_at_to'"
        raise ValueError(msg)
    if updated_at is not None:
        start, stop = _parse_datetime_param(updated_at)
        params["updatedAtFrom"] = start.isoformat()
        params["updatedAtTo"] = stop.isoformat()
    if updated_at_from is not None:
        params["updatedAtFrom"] = _parse_datetime_param(updated_at_from)[0].isoformat()
    if updated_at_to is not None:
        params["updatedAtTo"] = _parse_datetime_param(updated_at_to)[1].isoformat()


def _parse_date_param(date: DateParam) -> tuple[datetime.date, datetime.date]:
    if isinstance(date, datetime.date):
        return date, date
    error = ValueError(f"Invalid date format: {date}")
    if isinstance(date, str):
        try:
            parts = [int(part) for part in date.split("-")]
        except ValueError:
            raise error from None
        match parts:
            case [year, month, day]:
                date = datetime.date(year, month, day)
                return date, date
            case [year, month]:
                last_day_number = calendar.monthrange(year, month)[1]
                return datetime.date(year, month, 1), datetime.date(
                    year, month, last_day_number
                )
            case [year]:
                return datetime.date(year, 1, 1), datetime.date(year, 12, 31)
    raise error


def _parse_datetime_param(
    dt: DateTimeParam,
) -> tuple[datetime.datetime, datetime.datetime]:
    if isinstance(dt, datetime.datetime):
        return dt, dt
    if isinstance(dt, datetime.date):
        return datetime.datetime.combine(
            dt, datetime.time(0, 0, 0, 0)
        ), datetime.datetime.combine(dt, datetime.time(23, 59, 59, 999999))
    if isinstance(dt, str):
        patterns = {
            ("%Y", "years"),
            ("%Y-%m", "months"),
            ("%Y-%m-%d", "days"),
            ("%Y-%m-%dT%H", "hours"),
            ("%Y-%m-%dT%H:%M", "minutes"),
            ("%Y-%m-%dT%H:%M:%S", "seconds"),
            ("%Y-%m-%dT%H:%M:%S.%f", "microseconds"),
        }
        for fmt, unit in patterns:
            try:
                start_date = datetime.datetime.strptime(dt, fmt)
            except ValueError:
                continue
            if unit == "years":
                end_date = start_date.replace(year=start_date.year + 1)
            elif unit == "months":
                if start_date.month == 12:
                    end_date = start_date.replace(year=start_date.year + 1, month=1)
                else:
                    end_date = start_date.replace(month=start_date.month + 1)
            elif unit == "days":
                end_date = start_date + datetime.timedelta(days=1)
            elif unit == "hours":
                end_date = start_date + datetime.timedelta(hours=1)
            elif unit == "minutes":
                end_date = start_date + datetime.timedelta(minutes=1)
            elif unit == "seconds":
                end_date = start_date + datetime.timedelta(seconds=1)
            elif unit == "microseconds":
                return start_date, start_date
            return start_date, end_date - datetime.timedelta(microseconds=1)
    msg = f"Invalid datetime format: {dt}"
    raise ValueError(msg)


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
    field_names = (
        {f.name for f in fields(ProductMetadata)}
        - CONVERTED
        - {"product", "instrument", "model", "site"}
    )
    return [
        ProductMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            product=Product(
                id=obj["product"]["id"],
                human_readable_name=obj["product"]["humanReadableName"],
                type=obj["product"]["type"],
                experimental=obj["product"]["experimental"],
            ),
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
    return [_build_object(d, cls) for d in data_list]


def _build_object(data: dict, cls: type[T]) -> T:
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
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _set_of_ids(res: dict, name: str) -> frozenset[str]:
    return frozenset(obj["id"] for obj in res.get(name, []))


def _make_session() -> requests.Session:
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
    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ")


def _check_params(params: dict, ignore: tuple = ()) -> None:
    if sum(1 for key, value in params.items() if key not in ignore and value) == 0:
        raise TypeError("At least one of the parameters must be set.")


def validate_type(type, values) -> list | None:
    if type is not None:
        if not isinstance(type, str | list):
            raise ValueError(f"Invalid type: {type}")
        type = [type] if isinstance(type, str) else type
        for t in type:
            if t not in get_args(values):
                raise ValueError(f"Invalid type: {t}")
    return type
