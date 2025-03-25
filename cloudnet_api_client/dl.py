import asyncio
import logging
import os
from os import PathLike
from pathlib import Path

import aiohttp

from cloudnet_api_client import utils
from cloudnet_api_client.containers import Metadata, RawMetadata


def download(
    metadata: list[Metadata], output_directory: PathLike, concurrency_limit: int = 5
) -> None:
    os.makedirs(output_directory, exist_ok=True)
    asyncio.run(_download_files(metadata, output_directory, concurrency_limit))


async def _download_files(
    metadata: list[Metadata], output_path: PathLike, concurrency_limit: int
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
                _download_file(session, meta.download_url, destination, semaphore)
            )
            tasks.append(task)
        await asyncio.gather(*tasks)


def _file_checksum_matches(meta: Metadata, destination: Path) -> bool:
    fun = utils.md5sum if isinstance(meta, RawMetadata) else utils.sha256sum
    return fun(destination) == meta.checksum


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
