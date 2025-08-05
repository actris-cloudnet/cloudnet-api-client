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
        _submit_file(
            "20250801_Magurele_CHM170137_000.nc",
            "bucharest",
            "chm15k",
            "2025-08-01",
            "https://hdl.handle.net/21.12132/3.c60c931fac9d43f0",
        )

    def test_raw_metadata(self):
        raw_metadata = self.client.raw_metadata(site_id="bucharest", date="2025-08-01")
        assert isinstance(raw_metadata, list)
        assert len(raw_metadata) == 1
        assert isinstance(raw_metadata[0], RawMetadata)


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
    res.raise_for_status()

    with open(file_path, "rb") as f:
        res = requests.put(f"{BACKEND_URL}/upload/data/{checksum}", data=f, auth=auth)
        res.raise_for_status()
