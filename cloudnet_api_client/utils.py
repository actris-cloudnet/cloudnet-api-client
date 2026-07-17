"""Utility functions for the Cloudnet API client.

This module provides exception classes and hash calculation utilities.
"""

import base64
import hashlib
from os import PathLike
from typing import Literal


class CloudnetAPIError(Exception):
    """Exception raised for errors from the Cloudnet API.

    Attributes:
        message: The error message from the API.
    """

    def __init__(self, msg: str):
        """Initialize the CloudnetAPIError.

        Args:
            msg: The error message from the API.
        """
        self.message = msg
        super().__init__(self.message)


def sha256sum(filename: str | PathLike) -> str:
    """Calculate the SHA256 hash of a file.

    Args:
        filename: Path to the file to hash.

    Returns:
        Hexadecimal SHA256 hash string.
    """
    return _calc_hash_sum(filename, "sha256")


def md5sum(filename: str | PathLike, is_base64: bool = False) -> str:
    """Calculate the MD5 hash of a file.

    Args:
        filename: Path to the file to hash.
        is_base64: If True, return the hash as base64. If False (default),
            return as hexadecimal.

    Returns:
        MD5 hash string, either hexadecimal or base64 encoded.
    """
    return _calc_hash_sum(filename, "md5", is_base64)


def _calc_hash_sum(
    filename: str | PathLike, method: Literal["sha256", "md5"], is_base64: bool = False
) -> str:
    """Calculate a hash sum for a file using the specified method.

    Args:
        filename: Path to the file to hash.
        method: Hash algorithm to use. Must be "sha256" or "md5".
        is_base64: If True, return the hash as base64. If False (default),
            return as hexadecimal.

    Returns:
        Hash string, either hexadecimal or base64 encoded.
    """
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.b64encode(hash_sum.digest()).decode("utf-8")
    return hash_sum.hexdigest()
