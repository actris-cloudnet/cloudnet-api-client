import hashlib
from pathlib import Path

import requests

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import Instrument, Product, RawMetadata, Site

BACKEND_URL = "http://localhost:3000"
DATA_PATH = Path(__file__).parent / "data"


class TestFixtures:
    def setup_method(self):
        self.client = APIClient(base_url=f"{BACKEND_URL}/api/")

    def test_sites(self):
        sites = self.client.sites()
        assert isinstance(sites[0], Site)

    def test_site_filter_cloudnet(self):
        sites = self.client.sites(type="cloudnet")
        assert all("cloudnet" in site.type for site in sites)

    def test_site_filter_hidden(self):
        sites = self.client.sites(type="hidden")
        assert all("hidden" in site.type for site in sites)
        assert all("cloudnet" not in site.type for site in sites)

    def test_products(self):
        products = self.client.products()
        assert isinstance(products[0], Product)

    def test_instruments(self):
        instruments = self.client.instruments()
        assert isinstance(instruments[0], Instrument)


class TestWithRawFiles:
    def setup_method(self):
        self.client = APIClient(base_url=f"{BACKEND_URL}/api/")
        metadata_list = [
            (
                "20250801_Magurele_CHM170137_000.nc",
                "bucharest",
                "chm15k",
                "2025-08-01",
                "https://hdl.handle.net/21.12132/3.c60c931fac9d43f0",
            ),
            (
                "20250808_Granada_CHM170119_0045_000.nc",
                "granada",
                "chm15k",
                "2025-08-08",
                "https://hdl.handle.net/21.12132/3.77a75f3b32294855",
            ),
            (
                "20250803_JOYCE_WST_01m.dat",
                "juelich",
                "weather-station",
                "2025-08-01",
                "https://hdl.handle.net/21.12132/3.726b3b29de1949cc",
            ),
        ]

        for item in metadata_list:
            _submit_file(*item)

    def test_raw_metadata_1(self):
        meta = self.client.raw_metadata(site_id="bucharest", date="2025-08-01")
        assert isinstance(meta, list)
        assert len(meta) == 1
        assert isinstance(meta[0], RawMetadata)

    def test_raw_metadata_2(self):
        meta = self.client.raw_metadata(date="2025-08-08")
        assert len(meta) == 1

    def test_raw_metadata_3(self):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = self.client.raw_metadata(instrument_pid=pid)
        assert len(meta) == 1

    def test_raw_metadata_4(self):
        pid = "https://hdl.handle.net/21.12132/3.77a75f3b32294855"
        meta = self.client.raw_metadata(instrument_pid=pid, date="2022-01-01")
        assert len(meta) == 0

    def test_raw_metadata_5(self):
        meta = self.client.raw_metadata(date_from="2025-08-01")
        assert len(meta) == 3

    def test_raw_metadata_6(self):
        meta = self.client.raw_metadata(date_from="2025-08-01", date_to="2025-08-08")
        assert len(meta) == 3

    def test_raw_metadata_7(self):
        meta = self.client.raw_metadata(date_from="2025-08-01", date_to="2025-08-07")
        assert len(meta) == 2

    def test_raw_metadata_8(self):
        meta = self.client.raw_metadata(filename_prefix="20250801")
        assert len(meta) == 1

    def test_raw_metadata_9(self):
        meta = self.client.raw_metadata(filename_suffix="000.nc")
        assert len(meta) == 2

    def test_raw_metadata_10(self):
        meta = self.client.raw_metadata(instrument_id="weather-station")
        assert len(meta) == 1


def _submit_file(filename: str, site: str, instrument: str, date: str, pid: str):
    auth = ("admin", "admin")
    file_path = DATA_PATH / filename

    with open(file_path, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    metadata = {
        "filename": filename,
        "checksum": checksum,
        "site": site,
        "instrument": instrument,
        "measurementDate": date,
        "instrumentPid": pid,
    }

    res = requests.post(f"{BACKEND_URL}/upload/metadata/", json=metadata, auth=auth)
    if res.status_code not in (200, 409):
        res.raise_for_status()

    if res.status_code == 200:
        with open(file_path, "rb") as f:
            res = requests.put(
                f"{BACKEND_URL}/upload/data/{checksum}", data=f, auth=auth
            )
            res.raise_for_status()
