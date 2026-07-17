"""Download functionality for the Cloudnet API client.

This module provides asynchronous download capabilities for fetching files
from the Cloudnet data portal API.
"""

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from tqdm import tqdm

from cloudnet_api_client import utils
from cloudnet_api_client.containers import (
    Metadata,
    ProductMetadata,
)


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

    url: str
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
        validate_checksum: Whether to validate file checksums after download.

    Returns:
        List of Path objects pointing to the downloaded files.
    """
    metas = list(metadata) if isinstance(metadata, Iterable) else [metadata]
    file_exists = _checksum_matches if validate_checksum else _size_and_name_matches
    semaphore = asyncio.Semaphore(concurrency_limit)
    total_bytes = sum(meta.size for meta in metas)
    bar_config = BarConfig(disable_progress, concurrency_limit, total_bytes, len(metas))
    full_paths = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for meta in metas:
            download_url = f"{base_url}{meta.download_url.split('/api/')[-1]}"
            destination = output_path / meta.download_url.split("/")[-1]
            full_paths.append(destination)
            if destination.exists() and file_exists(meta, destination):
                logging.debug(f"Already downloaded: {destination}")
                continue
            dl_params = DlParams(
                url=download_url,
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
    """
    position = await params.bar_config.position_queue.get()
    try:
        for attempt in range(1, max_retries + 1):
            try:
                await _download_file(params, position)
                return
            except aiohttp.ClientError as e:
                logging.warning(f"Attempt {attempt} failed for {params.url}: {e}")
                if attempt == max_retries:
                    logging.error(
                        f"Giving up on {params.url} after {max_retries} attempts."
                    )
                    raise e
                else:
                    # Exponential backoff before retrying
                    await asyncio.sleep(2**attempt)
    finally:
        params.bar_config.position_queue.put_nowait(position)
    raise RuntimeError("Unreachable code reached.")


async def _download_file(
    params: DlParams,
    position: int,
) -> None:
    """Download a single file to the specified destination.

    Args:
        params: The download parameters.
        position: The position for the progress bar.

    Raises:
        aiohttp.ClientError: If the download fails.
        Exception: If file operations fail.
    """
    tmp_path = params.destination.with_suffix(f"{params.destination.suffix}.part")
    async with params.semaphore, params.session.get(params.url) as response:
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
                    bar.update(len(chunk))
                    params.bar_config.total_amount.update(len(chunk))
            tmp_path.replace(params.destination)
        except Exception:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            raise
        finally:
            bar.close()
            bar.clear()


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
