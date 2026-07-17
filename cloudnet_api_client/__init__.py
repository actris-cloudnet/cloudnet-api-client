"""Cloudnet API client package.

This package provides a Python client for interacting with the Cloudnet data portal API.
It includes functionality for fetching sites, products, instruments, models, and
file metadata, as well as downloading files.

Main classes:
    APIClient: The main client class for interacting with the Cloudnet API.

Exceptions:
    CloudnetAPIError: Exception raised for errors from the Cloudnet API.
"""

from .client import APIClient as APIClient
from .utils import CloudnetAPIError as CloudnetAPIError
