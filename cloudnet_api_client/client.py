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
    PRODUCT_TYPES,
    SITE_TYPES,
    Instrument,
    Metadata,
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
)

T = TypeVar("T")
type DateParam = str | datetime.date | None


class APIClient:
    def __init__(
        self,
        base_url: str = "https://cloudnet.fmi.fi/api/",
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.session = session or _make_session()

    def sites(self, type: SITE_TYPES | None = None) -> list[Site]:
        params = {"type": type} if type else None
        res = self._get_response("sites", params)
        return _build_objects(res, Site)

    def products(self, type: PRODUCT_TYPES | None = None) -> list[Product]:
        res = self._get_response("products")
        data = _build_objects(res, Product)
        if type:
            data = [obj for obj in data if type in obj.type]
        return data

    def metadata(
        self,
        site_id: str,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        product: str | list[str] | None = None,
        instrument: str | list[str] | None = None,
        instrument_pid: str | list[str] | None = None,
        show_legacy: bool = False,
    ) -> list[ProductMetadata]:
        params = {
            "site": site_id,
            "product": product,
            "instrument": instrument,
            "instrumentPid": instrument_pid,
            "showLegacy": show_legacy,
        }
        date_params = self._mangle_dates(date, date_from, date_to)
        params.update(date_params)
        res = self._get_response("files", params)
        return _build_objects(res, ProductMetadata)

    def raw_metadata(
        self, site_id: str, date: datetime.date | str, instrument_pid: str | None = None
    ) -> list[RawMetadata]:
        params = {"site": site_id, "date": date, "instrumentPid": instrument_pid}
        res = self._get_response("raw-files", params)
        return _build_raw_meta_objects(res)

    @staticmethod
    def filter(
        meta: list[Metadata],
        include_pattern: str | None = None,
        exclude_pattern: str | None = None,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
    ) -> list[Metadata]:
        if include_pattern:
            meta = [m for m in meta if re.search(include_pattern, m.filename, re.I)]
        if exclude_pattern:
            meta = [m for m in meta if not re.search(exclude_pattern, m.filename, re.I)]
        if filename_prefix:
            meta = [m for m in meta if m.filename.startswith(filename_prefix)]
        if filename_suffix:
            meta = [m for m in meta if m.filename.endswith(filename_suffix)]
        if include_tag_subset:
            meta = [
                m
                for m in meta
                if isinstance(m, RawMetadata)
                and m.tags
                and include_tag_subset.issubset(m.tags)
            ]
        if exclude_tag_subset:
            meta = [
                m
                for m in meta
                if isinstance(m, RawMetadata)
                and m.tags
                and not exclude_tag_subset.issubset(m.tags)
            ]
        return meta

    def _get_response(self, endpoint: str, params: dict | None = None) -> list[dict]:
        url = urljoin(self.base_url, endpoint)
        res = self.session.get(url, params=params, timeout=120)
        res.raise_for_status()
        return res.json()

    def _mangle_dates(
        self, date: DateParam, date_from: DateParam, date_to: DateParam
    ) -> dict:
        params = {}
        if isinstance(date, str):
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
        elif isinstance(date, datetime.date):
            params["date"] = date
        else:
            if date_from:
                params["dateFrom"] = self._parse_date(date_from)
            if date_to:
                params["dateTo"] = self._parse_date(date_to)
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
        id=obj["instrumentInfo"]["instrumentId"],
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
