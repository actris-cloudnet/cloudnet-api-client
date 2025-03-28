import hashlib
from os import PathLike
from typing import Literal


def sha256sum(filename: str | PathLike) -> str:
    return _calc_hash_sum(filename, "sha256")


def md5sum(filename: str | PathLike) -> str:
    return _calc_hash_sum(filename, "md5")


def _calc_hash_sum(filename: str | PathLike, method: Literal["sha256", "md5"]) -> str:
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    return hash_sum.hexdigest()
