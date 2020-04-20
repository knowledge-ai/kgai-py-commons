"""
Dataclass to hold Google News Article
`Link sources https://newsapi.org/docs/endpoints/everything`_
"""

__author__ = 'Ritaja'

from dataclasses import dataclass
from typing import Optional

from kgai_py_commons.model.googlenews.source_article import SourceArticle


@dataclass
class NewsArticle:
    title: str
    publishedAt: str
    url: str
    # optional fields
    urlToImage: Optional[str]
    description: Optional[str]
    content: Optional[str]
    author: Optional[str]
    articleText: Optional[str]
    # complex objects
    source: SourceArticle
