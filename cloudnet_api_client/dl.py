"""Download functionality for the Cloudnet API client.

This module provides asynchronous download capabilities for fetching files
from the Cloudnet data portal API.
"""

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from tqdm import tqdm

from cloudnet_api_client import utils
from cloudnet_api_client.containers import (
    Metadata,
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
)

logger = logging.getLogger(__name__)


class BarConfig:
    """Configuration for download progress bars.

    Attributes:
        disable: Whether progress bars are disabled.
        single_file: Whether only a single file is being downloaded.
        position_queue: Queue for managing progress bar positions.
        total_amount: The main progress bar for total download progress.
        lock: Asyncio lock for thread-safe operations.
    """

    def __init__(
        self,
        disable: bool | None,
        max_workers: int,
        total_bytes: int,
        n_files: int,
    ) -> None:
        """Initialize the BarConfig.

        Args:
            disable: Whether progress bars are disabled.
            max_workers: Maximum number of concurrent workers.
            total_bytes: Total number of bytes to download.
            n_files: Number of files to download.
        """
        self.disable = disable
        self.single_file = n_files <= 1
        self.position_queue = self._init_position_queue(max_workers)
        self.total_amount = tqdm(
            total=total_bytes,
            desc="Progress",
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            disable=self.disable if not self.single_file else True,
            position=0,
            leave=False,
            colour="green",
        )
        self.lock = asyncio.Lock()

    def _init_position_queue(self, max_workers: int) -> asyncio.Queue:
        """Initialize the position queue for progress bars.

        Args:
            max_workers: Maximum number of concurrent workers.

        Returns:
            Asyncio queue with positions for progress bars.
        """
        queue: asyncio.Queue = asyncio.Queue()
        start = 0 if self.single_file else 1
        for i in range(start, start + max_workers):
            queue.put_nowait(i)
        return queue


@dataclass
class DlParams:
    """Parameters for a download task.

    Attributes:
        url: The URL to download from.
        destination: The path where the file will be saved.
        session: The aiohttp client session to use.
        semaphore: The semaphore for limiting concurrency.
        bar_config: The progress bar configuration.
        disable: Whether progress bars are disabled.
    """

    base_url: str
    meta: Metadata
    destination: Path
    session: aiohttp.ClientSession
    semaphore: asyncio.Semaphore
    bar_config: BarConfig
    disable: bool | None


async def download_files(
    base_url: str,
    metadata: Iterable[Metadata] | Metadata,
    output_path: Path,
    concurrency_limit: int,
    disable_progress: bool | None,
    validate_checksum: bool = False,
) -> list[Path]:
    """Download multiple files asynchronously.

    Args:
        base_url: The base URL of the Cloudnet API.
        metadata: Metadata object or iterable of metadata objects to download.
        output_path: The directory where files will be saved.
        concurrency_limit: Maximum number of concurrent downloads.
        disable_progress: Whether to disable progress bars. If True, a simple
            text progress indicator will be shown. If False, detailed progress
            bars will be shown. If None, defaults to detailed progress bars for
            multiple files.
        validate_checksum: Whether to validate file checksums or just file sizes
            of existing files before download.

    Returns:
        List of Path objects pointing to the downloaded files.
    """
    metas = list(metadata) if isinstance(metadata, Iterable) else [metadata]
    file_exists = _checksum_matches if validate_checksum else _size_and_name_matches
    semaphore = asyncio.Semaphore(concurrency_limit)
    total_bytes = sum(meta.size for meta in metas)
    bar_config = BarConfig(disable_progress, concurrency_limit, total_bytes, len(metas))
    full_paths = []
    async with aiohttp.ClientSession(
        headers={"User-Agent": utils.USER_AGENT}
    ) as session:
        tasks = []
        for meta in metas:
            destination = output_path / meta.download_url.split("/")[-1]
            full_paths.append(destination)
            if destination.exists() and file_exists(meta, destination):
                logger.debug("Already downloaded: %s", destination)
                continue
            dl_params = DlParams(
                base_url=base_url,
                meta=meta,
                destination=destination,
                session=session,
                semaphore=semaphore,
                bar_config=bar_config,
                disable=disable_progress,
            )
            task = asyncio.create_task(_download_file_with_retries(dl_params))
            tasks.append(task)
        if disable_progress is True:
            print(f"Downloading {len(metas)} files...", end="", flush=True)
        await asyncio.gather(*tasks)
        bar_config.total_amount.close()
        bar_config.total_amount.clear()
    if disable_progress is True:
        print(" done.", flush=True)
    return full_paths


async def _download_file_with_retries(
    params: DlParams,
    max_retries: int = 3,
) -> None:
    """Download a file with automatic retries on failure.

    Args:
        params: The download parameters.
        max_retries: Maximum number of retry attempts. Defaults to 3.

    Raises:
        aiohttp.ClientError: If all retry attempts fail.
        asyncio.TimeoutError: If all retry attempts timeout.
        utils.InvalidChecksumError: If all retry attempts have invalid checksum.
    """
    position = await params.bar_config.position_queue.get()
    tmp_path = params.destination.with_suffix(f"{params.destination.suffix}.part")
    download_ok = False
    try:
        for attempt in range(1, max_retries + 1):
            try:
                await _download_file(params, position, tmp_path)
                download_ok = True
                return
            except utils.InvalidChecksumError as e:
                params.meta = await _refresh_meta(params)
                # No error if the downloaded file matches the refreshed
                # metadata.
                if params.meta.checksum == e.checksum:
                    download_ok = True
                    return
                error: Exception = e
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                error = e

            logger.warning(
                "Attempt %d failed for %s: %s", attempt, params.meta.filename, error
            )
            if attempt == max_retries:
                logger.error(
                    "Giving up on %s after %d attempts.",
                    params.meta.filename,
                    max_retries,
                )
                raise error
            else:
                # Exponential backoff before retrying
                await asyncio.sleep(2**attempt)
    finally:
        if download_ok:
            tmp_path.replace(params.destination)
        else:
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)
        params.bar_config.position_queue.put_nowait(position)


async def _download_file(
    params: DlParams,
    position: int,
    tmp_path: Path,
) -> None:
    """Download a single file to the specified destination.

    Args:
        params: The download parameters.
        position: The position for the progress bar.
        tmp_path: Temporary file for the download.

    Raises:
        aiohttp.ClientError: If the download fails.
        Exception: If file operations fail.
    """
    download_url = f"{params.base_url}{params.meta.download_url.split('/api/')[-1]}"
    hasher = (
        hashlib.sha256() if isinstance(params.meta, ProductMetadata) else hashlib.md5()
    )
    async with params.semaphore, params.session.get(download_url) as response:
        response.raise_for_status()
        bar = tqdm(
            desc=params.destination.name,
            total=response.content_length,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            disable=params.bar_config.disable,
            position=position,
            leave=False,
            colour="cyan",
        )
        try:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
                    hasher.update(chunk)
                    bar.update(len(chunk))
                    params.bar_config.total_amount.update(len(chunk))
        finally:
            bar.close()
            bar.clear()
    checksum = hasher.hexdigest()
    if checksum != params.meta.checksum:
        raise utils.InvalidChecksumError(checksum, params.meta)


async def _refresh_meta(params: DlParams) -> Metadata:
    if isinstance(params.meta, ProductMetadata):
        endpoint = "files"
        builder: Callable = utils._build_meta_objects
    elif isinstance(params.meta, RawMetadata):
        endpoint = "raw-files"
        builder = utils._build_raw_meta_objects
    elif isinstance(params.meta, RawModelMetadata):
        endpoint = "raw-model-files"
        builder = utils._build_raw_model_meta_objects
    else:
        msg = "Invalid metadata"
        raise ValueError(msg)
    url = f"{params.base_url}{endpoint}/{params.meta.uuid}"
    async with params.session.get(url) as res:
        res.raise_for_status()
        obj = await res.json()
        return builder([obj])[0]


def _checksum_matches(meta: Metadata, destination: Path) -> bool:
    """Check if a downloaded file matches its expected checksum.

    Args:
        meta: The metadata for the file.
        destination: The path to the downloaded file.

    Returns:
        True if the checksum matches, False otherwise.
    """
    fun = utils.sha256sum if isinstance(meta, ProductMetadata) else utils.md5sum
    return fun(destination) == meta.checksum


def _size_and_name_matches(meta: Metadata, destination: Path) -> bool:
    """Check if a downloaded file matches its expected size and name.

    Args:
        meta: The metadata for the file.
        destination: The path to the downloaded file.

    Returns:
        True if the size and name match, False otherwise.
    """
    return (
        destination.stat().st_size == meta.size
        and destination.name == meta.download_url.split("/")[-1]
    )
