import asyncio
import logging
import os
from os import PathLike
from pathlib import Path

import aiohttp

from cloudnet_api_client import utils
from cloudnet_api_client.containers import ProductMetadata, RawMetadata

MetadataList = list[ProductMetadata] | list[RawMetadata]


def download(
    metadata: MetadataList, output_directory: str | PathLike, concurrency_limit: int = 5
) -> None:
    os.makedirs(output_directory, exist_ok=True)
    asyncio.run(_download_files(metadata, output_directory, concurrency_limit))


async def _download_files(
    metadata: MetadataList, output_path: str | PathLike, concurrency_limit: int
) -> None:
    semaphore = asyncio.Semaphore(concurrency_limit)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for meta in metadata:
            destination = output_path / Path(meta.download_url.split("/")[-1])
            if destination.exists() and _file_checksum_matches(meta, destination):
                logging.info(f"Already downloaded: {destination}")
                continue
            task = asyncio.create_task(
                _download_file_with_retries(
                    session, meta.download_url, destination, semaphore
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks)


async def _download_file_with_retries(
    session: aiohttp.ClientSession,
    url: str,
    destination: Path,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> None:
    """Attempt to download a file, retrying up to max_retries times if needed."""
    for attempt in range(1, max_retries + 1):
        try:
            await _download_file(session, url, destination, semaphore)
            return
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            if attempt == max_retries:
                logging.error(f"Giving up on {url} after {max_retries} attempts.")
            else:
                # Exponential backoff before retrying
                await asyncio.sleep(2**attempt)


async def _download_file(
    session: aiohttp.ClientSession,
    url: str,
    destination: Path,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        async with session.get(url) as response:
            response.raise_for_status()
            with destination.open("wb") as file_out:
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    file_out.write(chunk)
        logging.info(f"Downloaded: {destination}")


def _file_checksum_matches(
    meta: ProductMetadata | RawMetadata, destination: Path
) -> bool:
    fun = utils.md5sum if isinstance(meta, RawMetadata) else utils.sha256sum
    return fun(destination) == meta.checksum
