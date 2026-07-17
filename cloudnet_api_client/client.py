"""Cloudnet API client module.

This module provides the main APIClient class for interacting with the Cloudnet
data portal API. It includes methods for fetching sites, products, instruments,
models, files, and downloading data.
"""

import asyncio
import calendar
import datetime
import os
import re
import warnings
from collections.abc import Iterable
from dataclasses import asdict, fields, is_dataclass
from os import PathLike
from pathlib import Path
from platform import platform
from typing import TypeAlias, TypeVar, cast, get_args
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
    ExtendedProductMetadata,
    Instrument,
    MeanLocation,
    Model,
    Product,
    ProductMetadata,
    RawLocation,
    RawMetadata,
    RawModelMetadata,
    Site,
    Software,
    VersionMetadata,
)
from cloudnet_api_client.dl import download_files

from .utils import CloudnetAPIError
from .version import __version__

T = TypeVar("T")
MetadataList = (
    Iterable[ProductMetadata] | Iterable[RawMetadata] | Iterable[RawModelMetadata]
)
TMetadata = TypeVar("TMetadata", ProductMetadata, RawMetadata, RawModelMetadata)
DateParam = str | datetime.date | None
DateTimeParam = str | datetime.datetime | datetime.date | None
QueryParam: TypeAlias = T | Iterable[T] | None


class APIClient:
    """Client for the Cloudnet data portal API.

    This class provides methods to interact with the Cloudnet API, including
    fetching sites, products, instruments, models, and file metadata, as well
    as downloading files.

    Attributes:
        base_url: The base URL of the Cloudnet API.
        session: The requests session used for API calls.

    Example:
        >>> client = APIClient()
        >>> sites = client.sites()
        >>> files = client.files(site_id="hyytiala", date="2021-01-01")
    """

    def __init__(
        self,
        base_url: str = "https://cloudnet.fmi.fi/api/",
        session: requests.Session | None = None,
    ) -> None:
        """Initialize the APIClient.

        Args:
            base_url: The base URL of the Cloudnet API. Defaults to
                "https://cloudnet.fmi.fi/api/".
            session: An optional requests.Session to use for API calls.
                If not provided, a new session will be created.
        """
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.session = session or _make_session()

    def sites(self, type: QueryParam[SITE_TYPE] = None) -> list[Site]:
        """Fetch all sites from the Cloudnet API.

        Args:
            type: Optional filter for site types. Can be "cloudnet", "campaign",
                "model", "hidden", "mobile", "arm", "weather-radar", or "fmi-radar".
                Can be a single string or a list of strings.

        Returns:
            List of Site objects representing all sites matching the filter.

        Example:
            >>> client = APIClient()
            >>> all_sites = client.sites()
            >>> cloudnet_sites = client.sites(type="cloudnet")
        """
        type = _validate_type(type, SITE_TYPE)
        res = self._get("sites", {"type": type})
        return _build_objects(res, Site)

    def site(self, site_id: str) -> Site:
        """Fetch a single site by its ID.

        Args:
            site_id: The ID of the site to fetch.

        Returns:
            Site object representing the requested site.

        Raises:
            CloudnetAPIError: If the site with the given ID is not found.

        Example:
            >>> client = APIClient()
            >>> hyytiala = client.site("hyytiala")
        """
        res = self._get(f"sites/{site_id}")[0]
        return _build_object(res, Site)

    def products(self, type: QueryParam[PRODUCT_TYPE] = None) -> list[Product]:
        """Fetch all products from the Cloudnet API.

        Args:
            type: Optional filter for product types. Can be "instrument",
                "geophysical", "evaluation", or "model". Can be a single string
                or a list of strings.

        Returns:
            List of Product objects representing all products matching the filter.

        Example:
            >>> client = APIClient()
            >>> all_products = client.products()
            >>> instrument_products = client.products(type="instrument")
        """
        type = _validate_type(type, PRODUCT_TYPE)
        data = self._get("products")
        if type is not None:
            data = [obj for obj in data if any(t in obj["type"] for t in type)]
        return _build_objects(data, Product)

    def product(self, product_id: str) -> ExtendedProduct:
        """Fetch a single product by its ID.

        Args:
            product_id: The ID of the product to fetch.

        Returns:
            ExtendedProduct object containing the product details and related
            IDs for derived products, source instruments, and source products.

        Raises:
            CloudnetAPIError: If the product with the given ID is not found.

        Example:
            >>> client = APIClient()
            >>> classification = client.product("classification")
        """
        res = self._get(f"products/{product_id}")[0]
        obj = _build_object(res, Product)
        return ExtendedProduct(
            **asdict(obj),
            derived_product_ids=_set_of_ids(res, "derivedProducts"),
            source_instrument_ids=_set_of_ids(res, "sourceInstruments"),
            source_product_ids=_set_of_ids(res, "sourceProducts"),
        )

    def instruments(self) -> list[Instrument]:
        """Fetch all instruments from the Cloudnet API.

        Returns:
            List of Instrument objects representing all instruments.

        Example:
            >>> client = APIClient()
            >>> all_instruments = client.instruments()
        """
        res = self._get("instrument-pids")
        return [_create_instrument_object(obj) for obj in res]

    def instrument(self, uuid: str | UUID) -> ExtendedInstrument:
        """Fetch a single instrument by its UUID.

        Args:
            uuid: The UUID of the instrument to fetch.

        Returns:
            ExtendedInstrument object containing the instrument details and
            related derived product IDs.

        Raises:
            CloudnetAPIError: If the instrument with the given UUID is not found.

        Example:
            >>> client = APIClient()
            >>> instrument = client.instrument("d6bf209b-c48b-48a4-bbfb-fed713b27832")
        """
        res = self._get(f"instrument-pids/{uuid}")[0]
        obj = _create_instrument_object(res)
        return ExtendedInstrument(
            **asdict(obj),
            derived_product_ids=self.instrument_derived_products(obj.instrument_id),
        )

    def instrument_derived_products(self, instrument_id: str) -> frozenset[str]:
        """Fetch derived product IDs for a specific instrument.

        Args:
            instrument_id: The ID of the instrument.

        Returns:
            Frozenset of derived product IDs for the instrument.

        Raises:
            CloudnetAPIError: If the instrument with the given ID is not found.
        """
        res = self._get(f"instruments/{instrument_id}")[0]
        return _set_of_ids(res, "derivedProducts")

    def instrument_ids(self) -> frozenset[str]:
        """Fetch all instrument identifiers.

        Returns:
            Frozenset of all instrument IDs.

        Example:
            >>> client = APIClient()
            >>> all_ids = client.instrument_ids()
        """
        res = self._get("instruments")
        return frozenset(obj["id"] for obj in res)

    def models(self) -> list[Model]:
        """Fetch all models from the Cloudnet API.

        Returns:
            List of Model objects representing all available models.

        Example:
            >>> client = APIClient()
            >>> all_models = client.models()
        """
        res = self._get("models")
        return [_create_model_object(obj) for obj in res]

    def model(self, model_id: str) -> Model:
        """Fetch a single model by its ID.

        Args:
            model_id: The ID of the model to fetch.

        Returns:
            Model object representing the requested model.

        Raises:
            CloudnetAPIError: If the model with the given ID is not found.

        Example:
            >>> client = APIClient()
            >>> ecmwf = client.model("ecmwf-open")
        """
        res = self._get("models")
        model = [r for r in res if r["id"] == model_id]
        if not model:
            raise CloudnetAPIError(f"Model with id {model_id} not found")
        return _create_model_object(model[0])

    def file(
        self,
        uuid: str | UUID,
    ) -> ExtendedProductMetadata:
        """Fetch metadata of a single file by its UUID.

        Args:
            uuid: The UUID of the file to fetch.

        Returns:
            ExtendedProductMetadata object containing the file metadata and
            associated software information.

        Raises:
            CloudnetAPIError: If the file with the given UUID is not found.

        Example:
            >>> client = APIClient()
            >>> file_meta = client.file("405cc410-1f24-4ea9-bae8-da7f22be26cb")
        """
        file_res = self._get(f"files/{uuid}")[0]
        if file_res.get("instrument") is not None:
            instrument_uuid = file_res["instrument"]["uuid"]
            instrument_res = self._get(f"instrument-pids/{instrument_uuid}")[0]
        else:
            instrument_res = None
        obj = _build_meta_objects([file_res], instrument_res)[0]
        return ExtendedProductMetadata(
            **_asdict_shallow(obj),
            software=tuple(_build_objects(file_res["software"], Software)),
        )

    def versions(self, uuid: str | UUID) -> list[VersionMetadata]:
        """Fetch all version metadata for a specific file.

        Args:
            uuid: The UUID of the file to fetch versions for.

        Returns:
            List of VersionMetadata objects representing all versions of the file.

        Raises:
            CloudnetAPIError: If the file with the given UUID is not found.

        Example:
            >>> client = APIClient()
            >>> versions = client.versions("405cc410-1f24-4ea9-bae8-da7f22be26cb")
        """
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
        site_id: QueryParam[str] = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        instrument_id: QueryParam[str] = None,
        instrument_pid: QueryParam[str] = None,
        model_id: QueryParam[str] = None,
        product_id: QueryParam[str] = None,
        show_legacy: bool = False,
    ) -> list[ProductMetadata]:
        """Fetch product file metadata from the Cloudnet API.

        This method retrieves metadata for processed product files. It supports
        filtering by various criteria including site, date ranges, instruments,
        products, and models.

        Args:
            site_id: Site ID or list of site IDs to filter by.
            date: Date or date string (YYYY-MM-DD, YYYY-MM, or YYYY) to filter by.
                Cannot be used together with date_from or date_to.
            date_from: Start date for date range filtering.
            date_to: End date for date range filtering.
            updated_at: Updated at timestamp or date string to filter by.
                Supports formats: YYYY-MM-DD, YYYY-MM-DDTHH, YYYY-MM-DDTHH:MM,
                YYYY-MM-DDTHH:MM:SS, or YYYY-MM-DDTHH:MM:SS.FFFFFF.
            updated_at_from: Start timestamp for updated_at range filtering.
            updated_at_to: End timestamp for updated_at range filtering.
            instrument_id: Instrument ID or list of instrument IDs to filter by.
            instrument_pid: Instrument PID or list of instrument PIDs to filter by.
            model_id: Model ID or list of model IDs to filter by.
            product_id: Product ID or list of product IDs to filter by.
            show_legacy: Whether to include legacy files. Defaults to False.

        Returns:
            List of ProductMetadata objects matching the filter criteria.

        Raises:
            ValueError: If date is used together with date_from or date_to, or
                if updated_at is used together with updated_at_from or updated_at_to.
            TypeError: If at least one parameter is not set.
            CloudnetAPIError: If the API returns a 400 error.

        Example:
            >>> client = APIClient()
            >>> # Get all files for a site on a specific date
            >>> files = client.files(site_id="hyytiala", date="2021-01-01")
            >>> # Get files for multiple products
            >>> files = client.files(site_id="hyytiala", product_id=["mwr", "radar"])
        """
        params = {
            "site": site_id,
            "instrument": instrument_id,
            "instrumentPid": instrument_pid,
            "product": product_id,
            "model": model_id,
            "showLegacy": show_legacy,
        }
        if show_legacy is not True:
            # API shows legacy files with any value (even <False>)
            del params["showLegacy"]

        _add_date_params(
            params, date, date_from, date_to, updated_at, updated_at_from, updated_at_to
        )

        _check_params(params, ("showLegacy",))

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
            files_res += self._get("model-files", params, expected_code=400)

        return _build_meta_objects(files_res)

    def metadata(self, *args, **kwargs):
        """Fetch product file metadata (deprecated).

        Deprecated:
            Use files() instead. This method is maintained for backward
            compatibility and will be removed in a future version.

        Returns:
            List of ProductMetadata objects matching the filter criteria.
        """
        warnings.warn("use files instead of metadata", DeprecationWarning, stacklevel=2)
        return self.files(*args, **kwargs)

    def raw_files(
        self,
        site_id: QueryParam[str] = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        instrument_id: QueryParam[str] = None,
        instrument_pid: QueryParam[str] = None,
        filename_prefix: QueryParam[str] = None,
        filename_suffix: QueryParam[str] = None,
        status: QueryParam[STATUS] = None,
    ) -> list[RawMetadata]:
        """Fetch raw file metadata from the Cloudnet API.

        This method retrieves metadata for raw data files. It supports filtering
        by various criteria including site, date ranges, instruments, and file
        naming patterns.

        Args:
            site_id: Site ID or list of site IDs to filter by.
            date: Date or date string (YYYY-MM-DD, YYYY-MM, or YYYY) to filter by.
                Cannot be used together with date_from or date_to.
            date_from: Start date for date range filtering.
            date_to: End date for date range filtering.
            updated_at: Updated at timestamp or date string to filter by.
                Supports formats: YYYY-MM-DD, YYYY-MM-DDTHH, YYYY-MM-DDTHH:MM,
                YYYY-MM-DDTHH:MM:SS, or YYYY-MM-DDTHH:MM:SS.FFFFFF.
            updated_at_from: Start timestamp for updated_at range filtering.
            updated_at_to: End timestamp for updated_at range filtering.
            instrument_id: Instrument ID or list of instrument IDs to filter by.
            instrument_pid: Instrument PID or list of instrument PIDs to filter by.
            filename_prefix: Filename prefix or list of prefixes to filter by.
            filename_suffix: Filename suffix or list of suffixes to filter by.
            status: Status or list of statuses to filter by. Can be "created",
                "uploaded", "processed", or "invalid".

        Returns:
            List of RawMetadata objects matching the filter criteria.

        Raises:
            ValueError: If date is used together with date_from or date_to, or
                if updated_at is used together with updated_at_from or updated_at_to.
            CloudnetAPIError: If the API returns a 400 error.

        Example:
            >>> client = APIClient()
            >>> # Get all raw files for a site on a specific date
            >>> raw_files = client.raw_files(site_id="granada", date="2024-01")
            >>> # Get raw files for a specific instrument
            >>> raw_files = client.raw_files(site_id="granada", instrument_id="parsivel")
        """
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
        """Fetch raw file metadata (deprecated).

        Deprecated:
            Use raw_files() instead. This method is maintained for backward
            compatibility and will be removed in a future version.

        Returns:
            List of RawMetadata objects matching the filter criteria.
        """
        warnings.warn(
            "use raw_files instead of raw_metadata", DeprecationWarning, stacklevel=2
        )
        return self.raw_files(*args, **kwargs)

    def raw_model_files(
        self,
        site_id: QueryParam[str] = None,
        model_id: QueryParam[str] = None,
        date: DateParam = None,
        date_from: DateParam = None,
        date_to: DateParam = None,
        updated_at: DateTimeParam = None,
        updated_at_from: DateTimeParam = None,
        updated_at_to: DateTimeParam = None,
        filename_prefix: QueryParam[str] = None,
        filename_suffix: QueryParam[str] = None,
        status: QueryParam[STATUS] = None,
    ) -> list[RawModelMetadata]:
        """Fetch raw model file metadata from the Cloudnet API.

        This method is for internal CLU use only and may change in the future.
        It retrieves metadata for raw model data files.

        Args:
            site_id: Site ID or list of site IDs to filter by.
            model_id: Model ID or list of model IDs to filter by.
            date: Date or date string (YYYY-MM-DD, YYYY-MM, or YYYY) to filter by.
                Cannot be used together with date_from or date_to.
            date_from: Start date for date range filtering.
            date_to: End date for date range filtering.
            updated_at: Updated at timestamp or date string to filter by.
            updated_at_from: Start timestamp for updated_at range filtering.
            updated_at_to: End timestamp for updated_at range filtering.
            filename_prefix: Filename prefix or list of prefixes to filter by.
            filename_suffix: Filename suffix or list of suffixes to filter by.
            status: Status or list of statuses to filter by. Can be "created",
                "uploaded", "processed", or "invalid".

        Returns:
            List of RawModelMetadata objects matching the filter criteria.

        Raises:
            ValueError: If date is used together with date_from or date_to, or
                if updated_at is used together with updated_at_from or updated_at_to.
            TypeError: If at least one parameter is not set.
            CloudnetAPIError: If the API returns a 400 error.
        """
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
    ) -> MeanLocation:
        """Fetch the mean location of a moving site for a specific date.

        Args:
            site_id: The ID of the moving site.
            date: The date for which to fetch the location. Can be a date string
                in YYYY-MM-DD format or a datetime.date object.

        Returns:
            MeanLocation object containing the time, latitude, and longitude.

        Raises:
            CloudnetAPIError: If the site or location data is not found.

        Example:
            >>> client = APIClient()
            >>> location = client.moving_site_mean_location("ship", "2024-01-15")
        """
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"date": date}
        res = self._get(f"sites/{site_id}/locations", params=payload)[0]
        return MeanLocation(
            time=date,
            latitude=res["latitude"],
            longitude=res["longitude"],
        )

    def moving_site_locations(
        self, site_id: str, date: datetime.date | str
    ) -> list[RawLocation]:
        """Fetch raw location data for a moving site on a specific date.

        Args:
            site_id: The ID of the moving site.
            date: The date for which to fetch locations. Can be a date string
                in YYYY-MM-DD format or a datetime.date object.

        Returns:
            List of RawLocation objects containing timestamp, latitude, and
            longitude for each location point.

        Raises:
            CloudnetAPIError: If the site or location data is not found.

        Example:
            >>> client = APIClient()
            >>> locations = client.moving_site_locations("ship", "2024-01-15")
        """
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"date": date, "raw": "1"}
        locations = self._get(f"sites/{site_id}/locations", params=payload)
        return [
            RawLocation(
                time=_parse_datetime(location["date"]),
                latitude=location["latitude"],
                longitude=location["longitude"],
            )
            for location in locations
        ]

    def source_instruments(self, uuid: UUID | str) -> set[ExtendedInstrument]:
        """Recursively find all source instruments of a product file.

        This method traverses the source file chain to find all instruments
        that contributed to the creation of a product file.

        Args:
            uuid: The UUID of the product file to analyze.

        Returns:
            Set of ExtendedInstrument objects representing all source instruments.

        Raises:
            CloudnetAPIError: If the file with the given UUID is not found.

        Example:
            >>> client = APIClient()
            >>> sources = client.source_instruments("405cc410-1f24-4ea9-bae8-da7f22be26cb")
        """
        instruments = set()
        res = self._get(f"files/{uuid}")[0]
        if res.get("instrument"):
            instrument = self.instrument(res["instrument"]["uuid"])
            instruments.add(instrument)
        for source_id in res.get("sourceFileIds", []):
            instruments |= self.source_instruments(source_id)
        return instruments

    def calibration(self, instrument_pid: str, date: datetime.date | str) -> dict:
        """Fetch calibration information for an instrument on a specific date.

        Args:
            instrument_pid: The PID of the instrument to fetch calibration for.
            date: The date for which to fetch calibration. Can be a date string
                in YYYY-MM-DD format or a datetime.date object.

        Returns:
            Dictionary containing the calibration information for the instrument.

        Raises:
            CloudnetAPIError: If the calibration data is not found.

        Example:
            >>> client = APIClient()
            >>> calib = client.calibration("https://hdl.handle.net/...", "2024-01-15")
        """
        if not isinstance(date, datetime.date):
            date = datetime.date.fromisoformat(date)
        payload = {"instrumentPid": instrument_pid, "date": date.isoformat()}
        return self._get("calibration", params=payload)[0]

    def download(
        self,
        metadata: MetadataList | TMetadata,
        output_directory: str | PathLike = ".",
        concurrency_limit: int = 5,
        progress: bool | None = None,
        validate_checksum: bool = False,
    ) -> list[Path]:
        """Download files from the fetched metadata.

        This is the synchronous version of the download method. For usage inside
        Jupyter notebooks or similar environments, use the asynchronous version
        adownload() instead.

        Args:
            metadata: Metadata object, list of ProductMetadata, RawMetadata, or
                RawModelMetadata objects to download.
            output_directory: Directory where files will be downloaded.
                Defaults to the current directory.
            concurrency_limit: Maximum number of concurrent downloads. Defaults to 5.
            progress: Whether to show download progress. If None, progress will be
                shown if downloading multiple files. If True, progress will always
                be shown. If False, progress will never be shown.
            validate_checksum: Whether to validate file checksums after download.
                Defaults to False.

        Returns:
            List of Path objects pointing to the downloaded files.

        Example:
            >>> client = APIClient()
            >>> files = client.files(site_id="hyytiala", date="2021-01-01")
            >>> file_paths = client.download(files, "data/")
        """
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
        metadata: MetadataList | TMetadata,
        output_directory: str | PathLike = ".",
        concurrency_limit: int = 5,
        progress: bool | None = None,
        validate_checksum: bool = False,
    ) -> list[Path]:
        """Asynchronously download files from the fetched metadata.

        This is the asynchronous version of the download method. Use this when
        running inside Jupyter notebooks or similar environments where the
        synchronous version would block.

        Args:
            metadata: Metadata object, list of ProductMetadata, RawMetadata, or
                RawModelMetadata objects to download.
            output_directory: Directory where files will be downloaded.
                Defaults to the current directory.
            concurrency_limit: Maximum number of concurrent downloads. Defaults to 5.
            progress: Whether to show download progress. If None, progress will be
                shown if downloading multiple files. If True, progress will always
                be shown. If False, progress will never be shown.
            validate_checksum: Whether to validate file checksums after download.
                Defaults to False.

        Returns:
            List of Path objects pointing to the downloaded files.

        Example:
            >>> client = APIClient()
            >>> files = client.files(site_id="hyytiala", date="2021-01-01")
            >>> file_paths = await client.adownload(files, "data/")
        """
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
        """Filter a list of metadata objects based on various criteria.

        This method provides additional filtering capabilities that are not
        supported natively by the Cloudnet API. It can filter by filename patterns
        and tag subsets (for RawMetadata objects).

        Args:
            metadata: List of ProductMetadata or RawMetadata objects to filter.
            include_pattern: Regular expression pattern to match against filenames.
                Only files with matching filenames will be included.
            exclude_pattern: Regular expression pattern to match against filenames.
                Files with matching filenames will be excluded.
            include_tag_subset: Set of tags that must all be present in the
                metadata's tags for inclusion. Only applies to RawMetadata objects.
            exclude_tag_subset: Set of tags that, if all are present in the
                metadata's tags, will cause exclusion. Only applies to RawMetadata
                objects.

        Returns:
            Filtered list of metadata objects matching all the specified criteria.

        Example:
            >>> client = APIClient()
            >>> files = client.raw_files(site_id="hyytiala")
            >>> # Filter by filename pattern
            >>> filtered = client.filter(files, include_pattern="stare")
            >>> # Filter by tags
            >>> filtered = client.filter(files, include_tag_subset={"tag1", "tag2"})
        """
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
        """Make a GET request to the Cloudnet API.

        Args:
            endpoint: The API endpoint to call (without base URL).
            params: Dictionary of query parameters to include in the request.
            expected_code: HTTP status code that, if received, will raise
                CloudnetAPIError instead of HTTPError. Defaults to 404.

        Returns:
            List of dictionaries containing the JSON response data.
            If the response is a single dict, it will be wrapped in a list.

        Raises:
            requests.exceptions.HTTPError: If the request fails with an
                unexpected HTTP error.
            CloudnetAPIError: If the request fails with the expected_code
                status code.
            requests.exceptions.Timeout: If the request times out.
        """
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
    """Add date and datetime parameters to the params dictionary.

    This function validates that mutually exclusive date parameters are not used
    together and adds the appropriate query parameters to the params dictionary.

    Args:
        params: Dictionary to which date parameters will be added.
        date: Date parameter for simple date filtering.
        date_from: Start date for date range filtering.
        date_to: End date for date range filtering.
        updated_at: Updated at timestamp for simple filtering.
        updated_at_from: Start timestamp for updated_at range filtering.
        updated_at_to: End timestamp for updated_at range filtering.

    Raises:
        ValueError: If date is used together with date_from or date_to, or
            if updated_at is used together with updated_at_from or updated_at_to.
    """
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
    """Parse a date parameter into start and end dates.

    This function supports various date string formats and returns a tuple of
    start and end dates for range queries.

    Args:
        date: Date parameter to parse. Can be a datetime.date object or a string
            in one of the following formats:
            - "YYYY-MM-DD" - returns (date, date)
            - "YYYY-MM" - returns (first day of month, last day of month)
            - "YYYY" - returns (Jan 1 of year, Dec 31 of year)

    Returns:
        Tuple of (start_date, end_date) for the specified date range.

    Raises:
        ValueError: If the date format is invalid.
    """
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
    """Parse a datetime parameter into start and end datetimes.

    This function supports various datetime string formats and returns a tuple
    of start and end datetimes for range queries.

    Args:
        dt: Datetime parameter to parse. Can be a datetime.datetime, datetime.date,
            or a string in one of the following formats:
            - "YYYY" - returns (Jan 1 00:00:00, Jan 1 next year 00:00:00)
            - "YYYY-MM" - returns (first day 00:00:00, first day next month 00:00:00)
            - "YYYY-MM-DD" - returns (day 00:00:00, next day 00:00:00)
            - "YYYY-MM-DDTHH" - returns (hour 00:00, next hour 00:00)
            - "YYYY-MM-DDTHH:MM" - returns (minute 00:00, next minute 00:00)
            - "YYYY-MM-DDTHH:MM:SS" - returns (second 00:00, next second 00:00)
            - "YYYY-MM-DDTHH:MM:SS.FFFFFF" - returns (microsecond, microsecond)

    Returns:
        Tuple of (start_datetime, end_datetime) for the specified time range.

    Raises:
        ValueError: If the datetime format is invalid.
    """
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
                start_date = datetime.datetime.strptime(dt, fmt).replace(
                    tzinfo=datetime.timezone.utc
                )
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
    """Build ProductMetadata objects from API response data.

    Args:
        res: List of dictionaries containing product file metadata from the API.
        instrument_meta: Optional dictionary containing instrument metadata to use
            instead of the instrument metadata in the response.

    Returns:
        List of ProductMetadata objects constructed from the response data.
    """
    field_names = (
        {f.name for f in fields(ProductMetadata)}
        - CONVERTED
        - {"product", "instrument", "model", "site", "software"}
    )
    return [
        ProductMetadata(
            **{_to_snake(k): v for k, v in obj.items() if _to_snake(k) in field_names},
            product=_build_object(obj["product"], Product),
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
    """Build RawMetadata objects from API response data.

    Args:
        res: List of dictionaries containing raw file metadata from the API.

    Returns:
        List of RawMetadata objects constructed from the response data.
    """
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
    """Build RawModelMetadata objects from API response data.

    Args:
        res: List of dictionaries containing raw model file metadata from the API.

    Returns:
        List of RawModelMetadata objects constructed from the response data.
    """
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
    """Create a Model object from API response data.

    Args:
        meta: Dictionary containing model metadata from the API.

    Returns:
        Model object constructed from the metadata.
    """
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
    """Create an Instrument object from API response data.

    Args:
        meta: Dictionary containing instrument metadata from the API.

    Returns:
        Instrument object constructed from the metadata.
    """
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
    """Build a list of dataclass objects from API response data.

    Args:
        data_list: List of dictionaries containing data from the API.
        cls: The dataclass type to construct objects from.

    Returns:
        List of dataclass objects constructed from the data.
    """
    return [_build_object(d, cls) for d in data_list]


def _build_object(data: dict, cls: type[T]) -> T:
    """Build a dataclass object from API response data.

    This function converts camelCase keys to snake_case and handles list fields
    by converting them to frozensets.

    Args:
        data: Dictionary containing data from the API.
        cls: The dataclass type to construct an object from.

    Returns:
        Dataclass object constructed from the data.
    """
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
    """Convert a camelCase or PascalCase string to snake_case.

    Args:
        name: String to convert.

    Returns:
        String in snake_case format.
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _set_of_ids(res: dict, name: str) -> frozenset[str]:
    """Extract a frozenset of IDs from a nested list in a dictionary.

    Args:
        res: Dictionary containing a list of objects with 'id' fields.
        name: Key in the dictionary where the list of objects is stored.

    Returns:
        Frozenset of ID strings from the list of objects.
    """
    return frozenset(obj["id"] for obj in res.get(name, []))


def _make_session() -> requests.Session:
    """Create a requests session configured for the Cloudnet API.

    The session includes a custom User-Agent header and retry strategy.

    Returns:
        Configured requests.Session object.
    """
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
    """Parse a datetime string from the API into a datetime object.

    Supports datetime strings with and without microseconds.

    Args:
        dt: Datetime string to parse.

    Returns:
        datetime.datetime object with UTC timezone.

    Raises:
        ValueError: If the datetime format is not recognized.
    """
    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=datetime.timezone.utc
        )
    except ValueError:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )


def _check_params(params: dict, ignore: tuple = ()) -> None:
    """Check that at least one parameter is set.

    Args:
        params: Dictionary of parameters to check.
        ignore: Tuple of parameter names to ignore in the check.

    Raises:
        TypeError: If no parameters (excluding ignored ones) are set.
    """
    if sum(1 for key, value in params.items() if key not in ignore and value) == 0:
        raise TypeError("At least one of the parameters must be set.")


def _asdict_shallow(obj) -> dict:
    """Convert a dataclass object to a shallow dictionary.

    Args:
        obj: Dataclass object to convert.

    Returns:
        Dictionary mapping field names to their values.
    """
    return dict((field.name, getattr(obj, field.name)) for field in fields(obj))


def _validate_type(values, literal) -> list | None:
    """Validate that values are within the allowed literal type values.

    Args:
        values: Value or list of values to validate. Can be a single string or
            a list of strings.
        literal: A typing.Literal type containing the allowed values.

    Returns:
        List of validated values, or None if values is None.

    Raises:
        ValueError: If any value is not in the allowed values.
    """
    if values is None:
        return None
    if isinstance(values, str):
        values = [values]
    allowed_values = get_args(literal)
    output = []
    for value in values:
        if value not in allowed_values:
            raise ValueError(f"Invalid type: {value}")
        output.append(value)
    return output
