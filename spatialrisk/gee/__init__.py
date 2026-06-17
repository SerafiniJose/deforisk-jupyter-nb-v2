"""Shared GEE catalogue + adapter public surface."""

from spatialrisk.gee.adapter import GEEAdapter
from spatialrisk.gee.catalogue import CATALOGUE, get_resolver, register

__all__ = ["GEEAdapter", "CATALOGUE", "get_resolver", "register"]
