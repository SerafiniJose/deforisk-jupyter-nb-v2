"""Enums for the sampling package."""
from enum import Enum


class SamplingStrategy(str, Enum):
    random = "random"
    stratified = "stratified"
    systematic = "systematic"


class AllocationMethod(str, Enum):
    equal = "equal"
    proportional = "proportional"
    deforisk = "deforisk"
