import datetime
import os
from pathlib import Path
from typing import NamedTuple
from uuid import UUID

import netCDF4
import pytest
import requests

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    ExtendedInstrument,
    ExtendedProduct,
    Instrument,
    Location,
    Model,
    Product,
    ProductMetadata,
    RawMetadata,
    Site,
    VersionMetadata,
)
from cloudnet_api_client.utils import CloudnetAPIError, md5sum, sha256sum


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
        File("20250821_limassol_parsivel_41582c49.nc", legacy=False, volatile=False),
        File("20250822_leipzig-lim_ecmwf-open.nc", legacy=False, volatile=True),
    ]


@pytest.fixture(scope="session", autouse=True)
def submit_raw_files(backend_url, data_path, files_raw):
    for file_meta in files_raw:
        _submit_raw_file(backend_url, data_path, file_meta)


@pytest.fixture(scope="session", autouse=True)
def submit_product_files(backend_url: str, data_path: Path, files_product: list[File]):
    for file_meta in files_product:
        _submit_product_file(backend_url, data_path, file_meta)


class TestSites:
    def test_sites_route(self, client: APIClient):
        sites = client.sites()
        assert len(sites) > 5
        assert isinstance(sites[0], Site)

    def test_sites_route_with_filter_cloudnet(self, client: APIClient):
        sites = client.sites(type="cloudnet")
        assert all("cloudnet" in site.type for site in sites)

    def test_sites_route_with_filter_hidden(self, client: APIClient):
        sites = client.sites(type="hidden")
        assert all("hidden" in site.type for site in sites)
        assert all("cloudnet" not in site.type for site in sites)

    def test_sites_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(ValueError):
            client.sites(type="xxx")  # type: ignore

    def test_site_route(self, client: APIClient):
        site = client.site("bucharest")
        assert isinstance(site, Site)

    def test_moving_site_mean_location(self, client: APIClient):
        location = client.moving_site_mean_location("boaty", "2022-01-01")
        assert isinstance(location, Location)
        assert location.time == datetime.date(2022, 1, 1)
        assert round(location.latitude) == 60
        assert round(location.longitude) == 25

    def test_moving_site_locations(self, client: APIClient):
        locations = client.moving_site_locations("boaty", "2022-01-01")
        assert isinstance(locations, list)
        assert all(isinstance(loc, Location) for loc in locations)

    def test_invalid_site_id(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.site("invalid-site-id")

    def test_sites_are_hashable(self, client: APIClient):
        sites = client.sites()
        for site in sites:
            hash(site)

    def test_site_is_hashable(self, client: APIClient):
        site = client.site("bucharest")
        hash(site)


class TestProducts:
    def test_products_route(self, client: APIClient):
        products = client.products()
        assert len(products) > 5
        assert isinstance(products[0], Product)
        assert not isinstance(products[0], ExtendedProduct)
        assert isinstance(products[0].type, frozenset)

    def test_products_route_with_type_filter(self, client: APIClient):
        products = client.products("instrument")
        assert len(products) > 5
        assert all(product.type == {"instrument"} for product in products)

    def test_products_route_with_type_filter_combo(self, client: APIClient):
        products = client.products(["instrument", "geophysical"])
        assert len(products) > 5
        assert all(
            product.type in [{"instrument"}, {"geophysical"}] for product in products
        )

    def test_product_route_with_invalid_input_1(self, client: APIClient):
        with pytest.raises(ValueError):
            client.products(type="xxx")  # type: ignore

    def test_product_route_with_invalid_input_2(self, client: APIClient):
        with pytest.raises(ValueError):
            client.products(type=32)  # type: ignore

    def test_product_route_with_categorize(self, client: APIClient):
        product = client.product("categorize")
        assert isinstance(product, ExtendedProduct)
        assert product.source_instrument_ids == set()
        for prod in ("classification", "iwc", "lwc"):
            assert product.derived_product_ids is not None
            assert prod in product.derived_product_ids
        for prod in ("radar", "lidar", "mwr"):
            assert product.source_product_ids is not None
            assert prod in product.source_product_ids
        assert isinstance(product.type, frozenset)
        assert isinstance(product.source_instrument_ids, frozenset)
        assert isinstance(product.source_product_ids, frozenset)
        assert isinstance(product.derived_product_ids, frozenset)

    def test_product_route_with_radar(self, client: APIClient):
        product = client.product("radar")
        assert isinstance(product, ExtendedProduct)
        assert product.derived_product_ids is not None
        assert "categorize" in product.derived_product_ids
        assert product.source_product_ids == set()
        assert product.source_instrument_ids is not None
        for instr in ("basta", "rpg-fmcw-94", "mira-35"):
            assert instr in product.source_instrument_ids

    def test_product_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.product("invalid-product-id")

    def test_products_are_hashable(self, client: APIClient):
        products = client.products()
        for product in products:
            hash(product)

    def test_product_is_hashable(self, client: APIClient):
        product = client.product("categorize")
        hash(product)


class TestModels:
    def test_models_route(self, client: APIClient):
        models = client.models()
        assert len(models) > 5
        assert isinstance(models[0], Model)

    def test_model_route(self, client: APIClient):
        model = client.model("arpege-12-23")
        assert isinstance(model, Model)
        assert model.forecast_start is not None

    def test_model_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.model("invalid-model-id")

    def test_models_are_hashable(self, client: APIClient):
        models = client.models()
        for model in models:
            hash(model)

    def test_model_is_hashable(self, client: APIClient):
        model = client.model("arpege-12-23")
        hash(model)


class TestInstruments:
    def test_instruments_route(self, client: APIClient):
        instruments = client.instruments()
        assert len(instruments) > 50
        assert isinstance(instruments[0], Instrument)

    def test_instrument_route(self, client: APIClient):
        instrument = client.instrument("12da536f-0d07-41ea-9ced-f6cdeb97198b")
        assert isinstance(instrument, ExtendedInstrument)
        assert instrument.instrument_id == "hatpro"

    def test_derived_product_ids(self, client: APIClient):
        instrument = client.instrument("12da536f-0d07-41ea-9ced-f6cdeb97198b")
        for prod in ("mwr", "mwr-l1c"):
            assert prod in instrument.derived_product_ids

    def test_owners_is_tuple(self, client: APIClient):
        instrument = client.instrument("12da536f-0d07-41ea-9ced-f6cdeb97198b")
        assert isinstance(instrument.owners, tuple)

    def test_instrument_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.instrument("invalid-instrument-id")

    def test_instruments_are_hashable(self, client: APIClient):
        instruments = client.instruments()
        for instrument in instruments:
            hash(instrument)

    def test_instrument_is_hashable(self, client: APIClient):
        instrument = client.instrument("12da536f-0d07-41ea-9ced-f6cdeb97198b")
        hash(instrument)


class TestProductFiles:
    def test_file_route_with_geophysical_product(self, client: APIClient):
        uuid = "8dcc865c-6920-49ce-a627-de045ec896e8"
        meta = client.file(uuid)
        assert isinstance(meta, ProductMetadata)
        assert str(meta.uuid) == uuid
        assert meta.instrument is None
        assert meta.model is None
        assert meta.product.id == "classification"

    def test_file_route_with_instrument_product(self, client: APIClient):
        uuid = "ab872770-9136-4e61-8958-31e62abdfb1b"
        meta = client.file(uuid)
        assert meta.model is None
        assert isinstance(meta.instrument, Instrument)
        assert meta.instrument.instrument_id == "parsivel"

    def test_file_route_with_model_product(self, client: APIClient):
        uuid = "277d54f0-d376-4448-a784-f1c8b819b46a"
        meta = client.file(uuid)
        assert meta.instrument is None
        assert isinstance(meta.model, Model)
        assert meta.model.id == "ecmwf-open"

    def test_versions_route(self, client: APIClient):
        uuid = "8dcc865c-6920-49ce-a627-de045ec896e8"
        meta = client.versions(uuid)
        assert len(meta) == 1
        assert isinstance(meta[0], VersionMetadata)
        assert str(meta[0].uuid) == uuid

    def test_product_option(self, client: APIClient):
        meta = client.files(site_id="hyytiala", product_id="iwc")
        assert len(meta) == 1

    def test_show_legacy_option(self, client: APIClient):
        meta = client.files(site_id="hyytiala", date="2014-02-05")
        assert len(meta) == 0
        meta = client.files(site_id="hyytiala", date="2014-02-05", show_legacy=True)
        assert len(meta) == 1

    def test_model_search(self, client: APIClient):
        meta = client.files(model_id="ecmwf-open")
        assert len(meta) == 1

    def test_instrument_search(self, client: APIClient):
        meta = client.files(instrument_id="parsivel")
        assert len(meta) == 1

    def test_files_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.files(site_id="invalid-site")

    def test_file_is_hashable(self, client: APIClient):
        uuid = "8dcc865c-6920-49ce-a627-de045ec896e8"
        meta = client.file(uuid)
        hash(meta)


class TestRawFiles:
    def test_filter_by_site_and_date(self, client: APIClient):
        meta = client.raw_files(site_id="bucharest", date="2025-08-01")
        assert len(meta) == 1
        assert isinstance(meta[0], RawMetadata)
        assert isinstance(meta[0].tags, frozenset)

    def test_filter_by_date_only(self, client: APIClient):
        meta = client.raw_files(date="2025-08-08")
        assert len(meta) == 1

    def test_filter_by_instrument_pid(self, client: APIClient):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = client.raw_files(instrument_pid=pid)
        assert len(meta) == 1

    def test_filter_by_instrument_pid_no_match(self, client: APIClient):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = client.raw_files(instrument_pid=pid, date="2022-01-01")
        assert len(meta) == 0

    def test_filter_by_date_range_from(self, client: APIClient):
        meta = client.raw_files(date_from="2025-08-01")
        assert len(meta) == 3

    def test_filter_by_date_range_inclusive(self, client: APIClient):
        meta = client.raw_files(date_from="2025-08-01", date_to="2025-08-08")
        assert len(meta) == 3

    def test_filter_by_date_range_exclusive(self, client: APIClient):
        meta = client.raw_files(date_from="2025-08-01", date_to="2025-08-07")
        assert len(meta) == 2

    def test_filter_by_filename_prefix(self, client: APIClient):
        meta = client.raw_files(filename_prefix="20250801")
        assert len(meta) == 1

    def test_filter_by_filename_suffix(self, client: APIClient):
        meta = client.raw_files(filename_suffix="000.nc")
        assert len(meta) == 2

    def test_filter_by_instrument_id(self, client: APIClient):
        meta = client.raw_files(instrument_id="weather-station")
        assert len(meta) == 1

    def test_instrument_id_vs_pid_exclusivity(self, client: APIClient):
        meta1 = client.raw_files(instrument_id="chm15k")
        pid = "https://hdl.handle.net/21.12132/3.c60c931fac9d43f0"
        meta2 = client.raw_files(instrument_pid=pid)
        assert len(meta1) > 1  # Multiple chm15k files
        assert len(meta2) == 1  # Specific PID

    def test_malformed_pid(self, client: APIClient):
        meta = client.raw_files(instrument_pid="not-a-valid-pid")
        assert len(meta) == 0

    def test_raw_files_route_with_invalid_input(self, client: APIClient):
        with pytest.raises(CloudnetAPIError):
            client.raw_files(site_id="invalid-site")

    def test_raw_files_are_hashable(self, client: APIClient):
        meta = client.raw_files(date_from="2025-08-01", date_to="2025-08-08")
        for m in meta:
            hash(m)


class TestDateParameterHandling:
    """Test various date parameter formats and edge cases."""

    def test_date_string_formats(self, client: APIClient):
        meta1 = client.raw_files(date="2025-08-01")
        meta2 = client.raw_files(date="2025-8-1")
        assert len(meta1) == len(meta2)

    def test_date_object_parameter(self, client: APIClient):
        date_obj = datetime.date(2025, 8, 1)
        meta = client.raw_files(date=date_obj)
        assert len(meta) >= 0

    def test_datetime_parameter(self, client: APIClient):
        datetime_obj = datetime.datetime(2025, 8, 1, 12, 30)
        meta = client.raw_files(updated_at=datetime_obj)
        assert len(meta) >= 0

    def test_invalid_date_format(self, client: APIClient):
        with pytest.raises(ValueError):
            client.raw_files(date="invalid-date")

    def test_date_range_validation(self, client: APIClient):
        # date_from > date_to should return empty results
        meta = client.raw_files(date_from="2025-08-10", date_to="2025-08-01")
        assert len(meta) == 0


class TestDownloadingFunctionality:
    def test_downloading(self, client: APIClient, tmp_path: Path):
        meta = client.raw_files(date_from="2025-08-01")
        assert len(meta) == 3
        paths = client.download(meta, output_directory=tmp_path, progress=False)
        assert len(paths) == 3
        for path in paths:
            assert path.exists()

    def test_download_with_custom_directory(self, client: APIClient, tmp_path: Path):
        meta = client.raw_files(date="2025-08-01", site_id="bucharest")
        custom_dir = tmp_path / "custom" / "nested"
        paths = client.download(meta, output_directory=custom_dir, progress=False)
        assert len(paths) == 1
        assert paths[0].parent == custom_dir
        assert paths[0].exists()

    def test_download_existing_file_skip(self, client: APIClient, tmp_path: Path):
        meta = client.raw_files(date="2025-08-01", site_id="bucharest")
        paths1 = client.download(meta, output_directory=tmp_path, progress=False)
        original_size = paths1[0].stat().st_size
        paths2 = client.download(meta, output_directory=tmp_path, progress=False)
        assert paths1 == paths2
        assert paths2[0].stat().st_size == original_size

    async def test_async_download(self, client: APIClient, tmp_path: Path):
        meta = client.raw_files(date_from="2025-08-01")
        assert len(meta) == 3
        paths = await client.adownload(meta, output_directory=tmp_path, progress=False)
        assert len(paths) == 3
        for path in paths:
            assert path.exists()


class TestFilterCombinations:
    def test_multiple_filters_combined(self, client: APIClient):
        meta = client.raw_files(
            site_id="bucharest",
            instrument_id="chm15k",
            date_from="2025-08-01",
            date_to="2025-08-01",
        )
        assert len(meta) == 1
        assert meta[0].site.id == "bucharest"

    def test_contradictory_filters(self, client: APIClient):
        meta = client.raw_files(site_id="bucharest", instrument_id="weather-station")
        assert len(meta) == 0

    def test_partial_filename_matches(self, client: APIClient):
        meta = client.raw_files(filename_prefix="2025", filename_suffix=".nc")
        assert len(meta) == 2


def test_get_instrument_derived_products(client: APIClient):
    assert client.instrument_derived_products("hatpro") == {"mwr", "mwr-l1c"}
    assert client.instrument_derived_products("pluvio") == {"rain-gauge"}
    assert client.instrument_derived_products("parsivel") == {"disdrometer"}


def _submit_product_file(backend_url: str, data_path: Path, meta: File):
    _date, site_id, product, *_rest = meta.filename.removesuffix(".nc").split("_")
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
            "instrumentPid": getattr(nc, "instrument_pid", None),
            **file_info,
        }
        payload["model"] = product if payload["product"] == "model" else None
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
