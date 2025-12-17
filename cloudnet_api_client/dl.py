import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from tqdm import tqdm

from cloudnet_api_client import utils
from cloudnet_api_client.containers import Metadata, ProductMetadata


class BarConfig:
    def __init__(self, quiet: bool | None, max_workers: int, total_bytes: int) -> None:
        self.quiet = quiet
        self.position_queue = self._init_position_queue(max_workers)
        self.total_amount = tqdm(
            total=total_bytes,
            desc="Total amount",
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            disable=self.quiet,
            position=0,
            leave=False,
            colour="green",
        )
        self.lock = asyncio.Lock()

    def _init_position_queue(self, max_workers: int) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        for i in range(1, max_workers + 1):
            queue.put_nowait(i)
        return queue


@dataclass
class DlParams:
    url: str
    destination: Path
    session: aiohttp.ClientSession
    semaphore: asyncio.Semaphore
    bar_config: BarConfig
    quiet: bool | None


async def download_files(
    base_url: str,
    metadata: Iterable[Metadata],
    output_path: Path,
    concurrency_limit: int,
    disable_progress: bool | None,
    validate_checksum: bool = False,
) -> list[Path]:
    metas = list(metadata)
    file_exists = _checksum_matches if validate_checksum else _size_and_name_matches
    semaphore = asyncio.Semaphore(concurrency_limit)
    total_bytes = sum(meta.size for meta in metas)
    bar_config = BarConfig(disable_progress, concurrency_limit, total_bytes)
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
                quiet=disable_progress,
            )
            task = asyncio.create_task(_download_file_with_retries(dl_params))
            tasks.append(task)
        await asyncio.gather(*tasks)
        bar_config.total_amount.close()
        bar_config.total_amount.clear()
    return full_paths


async def _download_file_with_retries(
    params: DlParams,
    max_retries: int = 3,
) -> None:
    """Attempt to download a file, retrying up to max_retries times if needed."""
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
    tmp_path = params.destination.with_suffix(f"{params.destination.suffix}.part")
    async with params.semaphore, params.session.get(params.url) as response:
        response.raise_for_status()
        bar = tqdm(
            desc=params.destination.name,
            total=response.content_length,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            disable=params.bar_config.quiet,
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
    fun = utils.sha256sum if isinstance(meta, ProductMetadata) else utils.md5sum
    return fun(destination) == meta.checksum


def _size_and_name_matches(meta: Metadata, destination: Path) -> bool:
    return (
        destination.stat().st_size == meta.size
        and destination.name == meta.download_url.split("/")[-1]
    )
