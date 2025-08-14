import os
from pathlib import Path
from typing import NamedTuple
from uuid import UUID

import netCDF4
import pytest
import requests

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    Instrument,
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
    VersionMetadata,
)
from cloudnet_api_client.utils import md5sum, sha256sum


class RawFile(NamedTuple):
    filename: str
    site: str
    instrument: str
    date: str
    pid: str


class File(NamedTuple):
    filename: str
    legacy: bool
    volatile: bool


@pytest.fixture(scope="session")
def backend_url() -> str:
    return os.getenv("BACKEND_URL", "http://localhost:3000")


@pytest.fixture(scope="session")
def client(backend_url) -> APIClient:
    return APIClient(base_url=f"{backend_url}/api/")


@pytest.fixture(scope="session")
def data_path() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def files_raw() -> list[RawFile]:
    return [
        RawFile(
            filename="20250801_Magurele_CHM170137_000.nc",
            site="bucharest",
            instrument="chm15k",
            date="2025-08-01",
            pid="https://hdl.handle.net/21.12132/3.c60c931fac9d43f0",
        ),
        RawFile(
            filename="20250808_Granada_CHM170119_0045_000.nc",
            site="granada",
            instrument="chm15k",
            date="2025-08-08",
            pid="https://hdl.handle.net/21.12132/3.77a75f3b32294855",
        ),
        RawFile(
            filename="20250803_JOYCE_WST_01m.dat",
            site="juelich",
            instrument="weather-station",
            date="2025-08-01",
            pid="https://hdl.handle.net/21.12132/3.726b3b29de1949cc",
        ),
    ]


@pytest.fixture(scope="session")
def files_product() -> list[File]:
    return [
        File("20250814_bucharest_classification.nc", legacy=False, volatile=True),
        File("20250808_hyytiala_iwc-Z-T-method.nc", legacy=False, volatile=False),
        File("20140205_hyytiala_classification.nc", legacy=True, volatile=False),
    ]


@pytest.fixture(scope="session", autouse=True)
def submit_raw_files(backend_url, data_path, files_raw):
    for file_meta in files_raw:
        _submit_raw_file(backend_url, data_path, file_meta)


@pytest.fixture(scope="session", autouse=True)
def submit_product_files(backend_url: str, data_path: Path, files_product: list[File]):
    for file_meta in files_product:
        _submit_product_file(backend_url, data_path, file_meta)


class TestBasicMetadata:
    def test_sites(self, client: APIClient):
        sites = client.sites()
        assert sites
        assert isinstance(sites[0], Site)

    def test_site_filter_cloudnet(self, client: APIClient):
        sites = client.sites(type="cloudnet")
        assert all("cloudnet" in site.type for site in sites)

    def test_site_filter_hidden(self, client: APIClient):
        sites = client.sites(type="hidden")
        assert all("hidden" in site.type for site in sites)
        assert all("cloudnet" not in site.type for site in sites)

    def test_products(self, client: APIClient):
        products = client.products()
        assert products
        assert isinstance(products[0], Product)

    def test_instruments(self, client: APIClient):
        instruments = client.instruments()
        assert instruments
        assert isinstance(instruments[0], Instrument)


class TestRawMetadata:
    def test_filter_by_site_and_date(self, client: APIClient):
        meta = client.raw_metadata(site_id="bucharest", date="2025-08-01")
        assert len(meta) == 1
        assert isinstance(meta[0], RawMetadata)

    def test_filter_by_date_only(self, client: APIClient):
        meta = client.raw_metadata(date="2025-08-08")
        assert len(meta) == 1

    def test_filter_by_instrument_pid(self, client: APIClient):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = client.raw_metadata(instrument_pid=pid)
        assert len(meta) == 1

    def test_filter_by_instrument_pid_no_match(self, client: APIClient):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = client.raw_metadata(instrument_pid=pid, date="2022-01-01")
        assert len(meta) == 0

    def test_filter_by_date_range_from(self, client: APIClient):
        meta = client.raw_metadata(date_from="2025-08-01")
        assert len(meta) == 3

    def test_filter_by_date_range_inclusive(self, client: APIClient):
        meta = client.raw_metadata(date_from="2025-08-01", date_to="2025-08-08")
        assert len(meta) == 3

    def test_filter_by_date_range_exclusive(self, client: APIClient):
        meta = client.raw_metadata(date_from="2025-08-01", date_to="2025-08-07")
        assert len(meta) == 2

    def test_filter_by_filename_prefix(self, client: APIClient):
        meta = client.raw_metadata(filename_prefix="20250801")
        assert len(meta) == 1

    def test_filter_by_filename_suffix(self, client: APIClient):
        meta = client.raw_metadata(filename_suffix="000.nc")
        assert len(meta) == 2

    def test_filter_by_instrument_id(self, client: APIClient):
        meta = client.raw_metadata(instrument_id="weather-station")
        assert len(meta) == 1

    def test_downloading(self, client: APIClient, tmp_path: Path):
        meta = client.raw_metadata(date_from="2025-08-01")
        assert len(meta) == 3
        paths = client.download(meta, output_directory=tmp_path, progress=False)
        assert len(paths) == 3
        for path in paths:
            assert path.exists()


class TestProductMeta:
    def test_file_route(self, client: APIClient):
        uuid = "8dcc865c-6920-49ce-a627-de045ec896e8"
        meta = client.file(uuid)
        assert isinstance(meta, ProductMetadata)
        assert str(meta.uuid) == uuid

    def test_versions_route(self, client: APIClient):
        uuid = "8dcc865c-6920-49ce-a627-de045ec896e8"
        meta = client.versions(uuid)
        assert len(meta) == 1
        assert isinstance(meta[0], VersionMetadata)
        assert str(meta[0].uuid) == uuid

    def test_product_option(self, client: APIClient):
        meta = client.metadata(site_id="hyytiala", product="iwc")
        assert len(meta) == 1

    def test_show_legacy_option(self, client: APIClient):
        meta = client.metadata(site_id="hyytiala", date="2014-02-05")
        assert len(meta) == 0
        meta = client.metadata(site_id="hyytiala", date="2014-02-05", show_legacy=True)
        assert len(meta) == 1

    def test_downloading(self, client: APIClient, tmp_path: Path):
        meta = client.metadata(date_from="2025-08-01")
        assert len(meta) == 2
        paths = client.download(meta, output_directory=tmp_path, progress=False)
        assert len(paths) == 2
        for path in paths:
            assert path.exists()


def _submit_product_file(backend_url: str, data_path: Path, meta: File):
    _date, site_id, product = meta.filename.removesuffix(".nc").split("_")
    full_path = data_path / meta.filename
    headers = {"content-md5": md5sum(full_path, is_base64=True)}
    bucket = f"cloudnet-product{'-volatile' if meta.volatile else ''}"
    url = f"http://localhost:5900/{bucket}/{meta.filename}"
    with open(full_path, "rb") as f:
        res = requests.put(url, data=f, auth=("test", "test"), headers=headers)
        res.raise_for_status()
        file_info = {
            "version": res.json().get("version", ""),
            "size": int(res.json()["size"]),
        }
    with netCDF4.Dataset(full_path, "r") as nc:
        year, month, day = str(nc.year), str(nc.month).zfill(2), str(nc.day).zfill(2)
        payload = {
            "product": getattr(nc, "cloudnet_file_type", product),
            "site": site_id,
            "measurementDate": f"{year}-{month}-{day}",
            "format": "HDF5 (NetCDF4)",
            "checksum": sha256sum(full_path),
            "volatile": meta.volatile,
            "legacy": meta.legacy,
            "uuid": str(UUID(nc.file_uuid)),
            "pid": nc.pid,
            **file_info,
        }
    url = f"{backend_url}/files/{meta.filename}"
    res = requests.put(url, json=payload)
    if res.status_code == 403:
        return
    res.raise_for_status()


def _submit_raw_file(backend_url: str, data_path: Path, meta: RawFile):
    auth = ("admin", "admin")
    file_path = data_path / meta.filename
    checksum = md5sum(file_path)
    metadata = {
        "filename": meta.filename,
        "checksum": checksum,
        "site": meta.site,
        "instrument": meta.instrument,
        "measurementDate": meta.date,
        "instrumentPid": meta.pid,
    }
    res = requests.post(f"{backend_url}/upload/metadata/", json=metadata, auth=auth)
    if res.status_code == 409:
        return
    res.raise_for_status()
    with open(file_path, "rb") as f:
        res = requests.put(f"{backend_url}/upload/data/{checksum}", data=f, auth=auth)
        res.raise_for_status()
