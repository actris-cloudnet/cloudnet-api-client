import base64
import hashlib
import os


def sha256sum(filename: str | os.PathLike) -> str:
    return _calc_hash_sum(filename, "sha256", is_base64=False)


def md5sum(filename: str | os.PathLike, *, is_base64: bool = False) -> str:
    return _calc_hash_sum(filename, "md5", is_base64=is_base64)


def _calc_hash_sum(filename, method, *, is_base64: bool) -> str:
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.encodebytes(hash_sum.digest()).decode("utf-8").strip()
    return hash_sum.hexdigest()
