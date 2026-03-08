from __future__ import annotations

from typing import Dict, Type

from ..base import BaseDatasetCollector
from .aihub import AIHubCollector
from .aws_odr import AwsOdrCollector
from .data_europa import DataEuropaCollector
from .data_gov import DataGovCollector
from .figshare import FigshareCollector
from .harvard_dataverse import HarvardDataverseCollector
from .huggingface import HuggingFaceCollector
from .kaggle import KaggleCollector
from .public_data_portal import PublicDataPortalCollector
from .zenodo import ZenodoCollector


COLLECTORS: Dict[str, Type[BaseDatasetCollector]] = {
    "huggingface": HuggingFaceCollector,
    "public_data_portal": PublicDataPortalCollector,
    "figshare": FigshareCollector,
    "harvard_dataverse": HarvardDataverseCollector,
    "kaggle": KaggleCollector,
    "aihub": AIHubCollector,
    "aws_odr": AwsOdrCollector,
    "zenodo": ZenodoCollector,
    "data_gov": DataGovCollector,
    "data_europa": DataEuropaCollector,
}


__all__ = [
    "COLLECTORS",
    "HuggingFaceCollector",
    "PublicDataPortalCollector",
    "FigshareCollector",
    "HarvardDataverseCollector",
    "KaggleCollector",
    "AIHubCollector",
    "AwsOdrCollector",
    "ZenodoCollector",
    "DataGovCollector",
    "DataEuropaCollector",
]
