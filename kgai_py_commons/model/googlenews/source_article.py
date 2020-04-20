"""
Dataclass for Google news source article
`Link sources https://newsapi.org/docs/endpoints/sources`_
"""

__author__ = "Ritaja"

from dataclasses import dataclass
from typing import Optional


@dataclass
class SourceArticle:
    name: str
    # Optional fields
    id: Optional[str]
    description: Optional[str]
    url: Optional[str]
    category: Optional[str]
    language: Optional[str]
    country: Optional[str]
