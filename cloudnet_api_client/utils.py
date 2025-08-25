import base64
import hashlib
from os import PathLike
from typing import Literal


class CloudnetAPIError(Exception):
    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


def sha256sum(filename: str | PathLike) -> str:
    return _calc_hash_sum(filename, "sha256")


def md5sum(filename: str | PathLike, is_base64: bool = False) -> str:
    return _calc_hash_sum(filename, "md5", is_base64)


def _calc_hash_sum(
    filename: str | PathLike, method: Literal["sha256", "md5"], is_base64: bool = False
) -> str:
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.b64encode(hash_sum.digest()).decode("utf-8")
    return hash_sum.hexdigest()
