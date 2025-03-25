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
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
)

T = TypeVar("T")


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
        date: datetime.date | str,
        product: str | list[str] | None = None,
    ) -> list[ProductMetadata]:
        params = {"site": site_id, "date": date, "product": product}
        res = self._get_response("files", params)
        return _build_objects(res, ProductMetadata)

    def raw_metadata(
        self, site_id: str, date: datetime.date | str, instrument_pid: str | None = None
    ) -> list[RawMetadata]:
        params = {"site": site_id, "date": date, "instrumentPid": instrument_pid}
        res = self._get_response("raw-files", params)
        return _build_raw_meta_objects(res)

    def _get_response(self, endpoint: str, params: dict | None = None) -> list[dict]:
        url = urljoin(self.base_url, endpoint)
        res = self.session.get(url, params=params, timeout=120)
        res.raise_for_status()
        return res.json()


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
