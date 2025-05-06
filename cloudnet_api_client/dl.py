import asyncio
import logging
from pathlib import Path

import aiohttp
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from cloudnet_api_client import utils
from cloudnet_api_client.containers import (
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
)


async def download_files(
    base_url: str,
    metadata: list[ProductMetadata] | list[RawMetadata] | list[RawModelMetadata],
    output_path: Path,
    concurrency_limit: int,
    disable_progress: bool | None,
    validate_checksum: bool = False,
) -> list[Path]:
    file_exists = _checksum_matches if validate_checksum else _size_and_name_matches
    semaphore = asyncio.Semaphore(concurrency_limit)
    full_paths = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for meta in metadata:
            download_url = f"{base_url}{meta.download_url.split('/api/')[-1]}"
            destination = output_path / meta.download_url.split("/")[-1]
            full_paths.append(destination)
            if destination.exists() and file_exists(meta, destination):
                logging.debug(f"Already downloaded: {destination}")
                continue
            task = asyncio.create_task(
                _download_file_with_retries(
                    session, download_url, destination, semaphore, disable_progress
                )
            )
            tasks.append(task)
        await tqdm_asyncio.gather(
            *tasks, desc="Completed files", disable=disable_progress
        )
    return full_paths


async def _download_file_with_retries(
    session: aiohttp.ClientSession,
    url: str,
    destination: Path,
    semaphore: asyncio.Semaphore,
    disable_progress: bool | None,
    max_retries: int = 3,
) -> None:
    """Attempt to download a file, retrying up to max_retries times if needed."""
    for attempt in range(1, max_retries + 1):
        try:
            await _download_file(session, url, destination, semaphore, disable_progress)
            return
        except aiohttp.ClientError as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            if attempt == max_retries:
                logging.error(f"Giving up on {url} after {max_retries} attempts.")
                raise e
            else:
                # Exponential backoff before retrying
                await asyncio.sleep(2**attempt)


async def _download_file(
    session: aiohttp.ClientSession,
    url: str,
    destination: Path,
    semaphore: asyncio.Semaphore,
    disable_progress: bool | None,
) -> None:
    async with semaphore:
        async with session.get(url) as response:
            response.raise_for_status()
            with (
                destination.open("wb") as file_out,
                tqdm(
                    desc=destination.name,
                    total=response.content_length,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                    disable=disable_progress,
                ) as bar,
            ):
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    file_out.write(chunk)
                    bar.update(len(chunk))
        logging.debug(f"Downloaded: {destination}")


def _checksum_matches(
    meta: ProductMetadata | RawMetadata | RawModelMetadata, destination: Path
) -> bool:
    fun = utils.sha256sum if isinstance(meta, ProductMetadata) else utils.md5sum
    return fun(destination) == meta.checksum


def _size_and_name_matches(
    meta: ProductMetadata | RawMetadata | RawModelMetadata, destination: Path
) -> bool:
    return (
        destination.stat().st_size == meta.size
        and destination.name == meta.download_url.split("/")[-1]
    )
