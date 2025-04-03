import asyncio
import calendar
import datetime
import os
import re
from dataclasses import fields, is_dataclass
from os import PathLike
from pathlib import Path
from typing import TypeVar, cast
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cloudnet_api_client.containers import (
    PRODUCT_TYPE,
    SITE_TYPE,
    STATUS,
    Instrument,
    Metadata,
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
)
from cloudnet_api_client.dl import download_files

T = TypeVar("T")
DateParam = str | datetime.date | None
DateTimeParam = str | datetime.datetime | datetime.date | None
QueryParam = str | list[str] | None
MetadataList = list[ProductMetadata] | list[RawMetadata]


class APIClient:
    def __init__(
        self,
        base_url: str = "https://cloudnet.fmi.fi/api/",
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.session = session or _make_session()

    def sites(self, type: SITE_TYPE | list[SITE_TYPE] | None = None) -> list[Site]:
        params = {"type": type}
        res = self._get_response("sites", params)
        return _build_objects(res, Site)

    def products(
        self, type: PRODUCT_TYPE | list[PRODUCT_TYPE] | None = None
    ) -> list[Product]:
        res = self._get_response("products")
        data = _build_objects(res, Product)
        if isinstance(type, str):
            data = [obj for obj in data if type in obj.type]
        elif isinstance(type, list):
            data = [obj for obj in data if any(t in obj.type for t in type)]
        return data

    def instruments(self) -> list[Instrument]:
        res = self._get_response("instrument-pids")
        return [
            Instrument(
                instrument_id=obj["instrument"]["id"],
                model=obj["model"],
                type=obj["type"],
                uuid=obj["uuid"],
                pid=obj["pid"],
                owners=obj["owners"],
                serial_number=obj["serialNumber"],
                name=obj["name"],
            )
            for obj in res
        ]

    def metadata(
        self,
        site_id: str,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        instrument_id: QueryParam = None,
        instrument_pid: QueryParam = None,
        model_id: QueryParam = None,
        product: QueryParam = None,
        show_legacy: bool = False,
    ) -> list[ProductMetadata]:
        params = {
            "site": site_id,
            "instrument": instrument_id,
            "instrumentPid": instrument_pid,
            "product": product,
            "showLegacy": show_legacy,
        }
        _add_date_params(
            params, date, date_from, date_to, updated_at, updated_at_from, updated_at_to
        )
        no_instrument = not instrument_id or instrument_pid
        if no_instrument and (not product and model_id):
            files_res = []
        else:
            files_res = self._get_response("files", params)

        # Add model files if requested
        if (
            (not product and no_instrument)
            or (product and "model" in product)
            or (model_id and (not product or "model" in product))
        ):
            for key in ("showLegacy", "product", "instrument", "instrumentPid"):
                del params[key]
            params["model"] = model_id
            files_res += self._get_response("model-files", params)

        return _build_meta_objects(files_res)

    def raw_metadata(
        self,
        site_id: str,
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
        res = self._get_response("raw-files", params)
        return _build_raw_meta_objects(res)

    def download(
        self,
        metadata: MetadataList,
        output_directory: str | PathLike,
        concurrency_limit: int = 5,
        progress: bool | None = None,
    ) -> list[Path]:
        return asyncio.run(
            self.adownload(metadata, output_directory, concurrency_limit, progress)
        )

    async def adownload(
        self,
        metadata: MetadataList,
        output_directory: str | PathLike,
        concurrency_limit: int = 5,
        progress: bool | None = None,
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
        )

    @staticmethod
    def filter(
        metadata: list[Metadata],
        include_pattern: str | None = None,
        exclude_pattern: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
    ) -> list[Metadata]:
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
                if isinstance(m, RawMetadata)
                and m.tags
                and include_tag_subset.issubset(m.tags)
            ]
        if exclude_tag_subset:
            metadata = [
                m
                for m in metadata
                if isinstance(m, RawMetadata)
                and m.tags
                and not exclude_tag_subset.issubset(m.tags)
            ]
        return metadata

    def _get_response(self, endpoint: str, params: dict | None = None) -> list[dict]:
        url = urljoin(self.base_url, endpoint)
        res = self.session.get(url, params=params, timeout=120)
        res.raise_for_status()
        return res.json()


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
        start, stop = _parse_date(date)
        params["dateFrom"] = start.isoformat()
        params["dateTo"] = stop.isoformat()
    if date_from is not None:
        params["dateFrom"] = _parse_date(date_from)[0].isoformat()
    if date_to is not None:
        params["dateTo"] = _parse_date(date_to)[1].isoformat()

    if updated_at is not None and (
        updated_at_from is not None or updated_at_to is not None
    ):
        msg = "Cannot use 'updated_at' with 'updated_at_from' and 'updated_at_to'"
        raise ValueError(msg)
    if updated_at is not None:
        start, stop = _parse_datetime(updated_at)
        params["updatedAtFrom"] = start.isoformat()
        params["updatedAtTo"] = stop.isoformat()
    if updated_at_from is not None:
        params["updatedAtFrom"] = _parse_datetime(updated_at_from)[0].isoformat()
    if updated_at_to is not None:
        params["updatedAtTo"] = _parse_datetime(updated_at_to)[1].isoformat()


def _parse_date(date: DateParam) -> tuple[datetime.date, datetime.date]:
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


def _parse_datetime(dt: DateTimeParam) -> tuple[datetime.datetime, datetime.datetime]:
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


def _build_objects(res: list[dict], object_type: type[T]) -> list[T]:
    assert is_dataclass(object_type)
    field_names = {f.name for f in fields(object_type)}
    objects = [
        object_type(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names}
        )
        for obj in res
    ]
    return cast(list[T], objects)


def _build_meta_objects(res: list[dict]) -> list[ProductMetadata]:
    field_names = {f.name for f in fields(ProductMetadata)} - {"product"}
    return [
        ProductMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            product=Product(
                id=obj["product"]["id"],
                human_readable_name=obj["product"]["humanReadableName"],
                type=[obj["product"]["type"][1:-1]],
                experimental=obj["product"]["experimental"],
            ),
        )
        for obj in res
    ]


def _build_raw_meta_objects(res: list[dict]) -> list[RawMetadata]:
    field_names = {f.name for f in fields(RawMetadata)} - {"instrument"}
    return [
        RawMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            instrument=Instrument(
                instrument_id=obj["instrumentInfo"]["instrumentId"],
                model=obj["instrumentInfo"]["model"],
                type=obj["instrumentInfo"]["type"],
                uuid=obj["instrumentInfo"]["uuid"],
                pid=obj["instrumentInfo"]["pid"],
                owners=obj["instrumentInfo"]["owners"],
                serial_number=obj["instrumentInfo"]["serialNumber"],
                name=obj["instrumentInfo"]["name"],
            ),
        )
        for obj in res
    ]


def _to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _make_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(total=10, backoff_factor=0.1, status_forcelist=[524])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
