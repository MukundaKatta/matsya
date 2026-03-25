"""
Matsya - AI-powered web crawler with LLM-guided extraction.

Named after the Fish Avatar from Hindu mythology, Matsya dives deep
into the web to extract structured data using intelligent pattern matching.
"""

__version__ = "0.1.0"

from matsya.core import (
    CrawlEngine,
    CrawlJob,
    PageContent,
    ContentExtractor,
    ExtractionRule,
    CrawlQueue,
)
from matsya.extractor import (
    StructuredExtractor,
    TableExtractor,
    ListExtractor,
    EntityExtractor,
)
from matsya.scheduler import CrawlScheduler, CrawlHistory
