import calendar
import datetime
import re
from dataclasses import fields, is_dataclass
from typing import TypeVar, cast
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cloudnet_api_client.containers import (
    PRODUCT_TYPE,
    SITE_TYPE,
    Instrument,
    Metadata,
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
)

T = TypeVar("T")
DateParam = str | datetime.date | None
QueryParam = str | list[str] | None


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

    def metadata(
        self,
        site_id: str,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at_from: DateParam = None,
        updated_at_to: DateParam = None,
        instrument_id: QueryParam = None,
        instrument_pid: QueryParam = None,
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
        date_params = self._mangle_dates(
            date, date_from, date_to, updated_at_from, updated_at_to
        )
        params.update(date_params)
        res = self._get_response("files", params)
        return _build_objects(res, ProductMetadata)

    def raw_metadata(
        self,
        site_id: str,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at_from: DateParam = None,
        updated_at_to: DateParam = None,
        instrument_id: QueryParam = None,
        instrument_pid: QueryParam = None,
    ) -> list[RawMetadata]:
        params = {
            "site": site_id,
            "instrument": instrument_id,
            "instrumentPid": instrument_pid,
        }
        date_params = self._mangle_dates(
            date, date_from, date_to, updated_at_from, updated_at_to
        )
        params.update(date_params)
        res = self._get_response("raw-files", params)
        return _build_raw_meta_objects(res)

    @staticmethod
    def filter(
        metadata: list[Metadata],
        include_pattern: str | None = None,
        exclude_pattern: str | None = None,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
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
        if filename_prefix:
            metadata = [m for m in metadata if m.filename.startswith(filename_prefix)]
        if filename_suffix:
            metadata = [m for m in metadata if m.filename.endswith(filename_suffix)]
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

    def _mangle_dates(
        self,
        date: DateParam,
        date_from: DateParam,
        date_to: DateParam,
        updated_at_from: DateParam,
        updated_at_to: DateParam,
    ) -> dict:
        params = {}
        if isinstance(date, datetime.date):
            params["date"] = date
        elif isinstance(date, str):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                params["date"] = self._parse_date(date)
            elif re.fullmatch(r"\d{4}-\d{2}", date):
                date = datetime.datetime.strptime(date, "%Y-%m")
                last_day_number = calendar.monthrange(date.year, date.month)[1]
                params["dateFrom"] = datetime.date(date.year, date.month, 1)
                params["dateTo"] = datetime.date(date.year, date.month, last_day_number)
            elif re.fullmatch(r"\d{4}", date):
                params["dateFrom"] = datetime.date(int(date), 1, 1)
                params["dateTo"] = datetime.date(int(date), 12, 31)
            else:
                raise ValueError("Invalid date format")
        else:
            if date_from:
                params["dateFrom"] = self._parse_date(date_from)
            if date_to:
                params["dateTo"] = self._parse_date(date_to)
        if updated_at_from:
            params["updatedAtFrom"] = self._parse_date(updated_at_from)
        if updated_at_to:
            params["updatedAtTo"] = self._parse_date(updated_at_to)
        return params

    @staticmethod
    def _parse_date(date: DateParam) -> datetime.date:
        if not date:
            raise ValueError("Date parameter is required")
        if isinstance(date, datetime.date):
            return date
        try:
            return datetime.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid date format: {date}") from e


def _build_objects(res: list[dict], object_type: type[T]) -> list[T]:
    assert is_dataclass(object_type)
    field_names = {f.name for f in fields(object_type)}
    instances = [
        object_type(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names}
        )
        for obj in res
    ]
    return cast(list[T], instances)


def _build_raw_meta_objects(res: list[dict]) -> list[RawMetadata]:
    field_names = {f.name for f in fields(RawMetadata)} - {"instrument"}
    return [
        RawMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            instrument=_construct_instrument(obj),
        )
        for obj in res
    ]


def _construct_instrument(obj: dict) -> Instrument:
    return Instrument(
        instrument_id=obj["instrumentInfo"]["instrumentId"],
        model=obj["instrumentInfo"]["model"],
        type=obj["instrumentInfo"]["type"],
        uuid=obj["instrumentInfo"]["uuid"],
        pid=obj["instrumentInfo"]["pid"],
        owners=obj["instrumentInfo"]["owners"],
        serial_number=obj["instrumentInfo"]["serialNumber"],
        name=obj["instrumentInfo"]["name"],
    )


def _to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _make_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(total=10, backoff_factor=0.1, status_forcelist=[524])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
