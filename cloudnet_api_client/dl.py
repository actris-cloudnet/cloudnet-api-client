import asyncio
import logging
import os
from os import PathLike
from pathlib import Path

import aiohttp
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from cloudnet_api_client import utils
from cloudnet_api_client.containers import ProductMetadata, RawMetadata

MetadataList = list[ProductMetadata] | list[RawMetadata]


def download(
    metadata: MetadataList,
    output_directory: str | PathLike,
    concurrency_limit: int = 5,
    progress: bool | None = None,
) -> list[Path]:
    return asyncio.run(
        adownload(metadata, output_directory, concurrency_limit, progress)
    )


async def adownload(
    metadata: MetadataList,
    output_directory: str | PathLike,
    concurrency_limit: int = 5,
    progress: bool | None = None,
) -> list[Path]:
    disable_progress = not progress if progress is not None else None
    output_directory = Path(output_directory).resolve()
    os.makedirs(output_directory, exist_ok=True)
    return await _download_files(
        metadata, output_directory, concurrency_limit, disable_progress
    )


async def _download_files(
    metadata: MetadataList,
    output_path: Path,
    concurrency_limit: int,
    disable_progress: bool | None,
) -> list[Path]:
    semaphore = asyncio.Semaphore(concurrency_limit)
    full_paths = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for meta in metadata:
            destination = output_path / meta.download_url.split("/")[-1]
            full_paths.append(destination)
            if destination.exists() and _file_checksum_matches(meta, destination):
                logging.info(f"Already downloaded: {destination}")
                continue
            task = asyncio.create_task(
                _download_file_with_retries(
                    session, meta.download_url, destination, semaphore, disable_progress
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
        logging.info(f"Downloaded: {destination}")


def _file_checksum_matches(
    meta: ProductMetadata | RawMetadata, destination: Path
) -> bool:
    fun = utils.md5sum if isinstance(meta, RawMetadata) else utils.sha256sum
    return fun(destination) == meta.checksum
