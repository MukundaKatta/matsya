"""
Core crawl engine, job definitions, content extraction, and queue management.

Provides the foundational components for Matsya's web crawling pipeline:
- CrawlJob: defines what to crawl and how to extract data
- PageContent: holds raw and processed page data
- ExtractionRule: pattern-based extraction rules
- ContentExtractor: applies rules to extract structured data from text
- CrawlQueue: priority-based job queue with deduplication
- CrawlEngine: orchestrates the full crawl-extract pipeline
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class OutputFormat(Enum):
    """Supported output formats for extracted data."""
    JSON = "json"
    CSV = "csv"
    TEXT = "text"


class CrawlStatus(Enum):
    """Status of a crawl job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CrawlJob:
    """
    Represents a single crawl task with target URL, CSS selectors for
    content extraction, and the desired output format.

    Attributes:
        url: Target URL to crawl.
        selectors: CSS-like selectors describing what to extract.
        output_format: Desired format for extracted data.
        priority: Job priority (lower number = higher priority).
        max_retries: Maximum retry attempts on failure.
        retry_count: Current number of retries attempted.
        status: Current job status.
        metadata: Arbitrary metadata attached to the job.
    """
    url: str
    selectors: List[str] = field(default_factory=list)
    output_format: str = "json"
    priority: int = 5
    max_retries: int = 3
    retry_count: int = 0
    status: CrawlStatus = CrawlStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def job_id(self) -> str:
        """Generate a deterministic job ID from URL and selectors."""
        key = f"{self.url}|{'|'.join(sorted(self.selectors))}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def can_retry(self) -> bool:
        """Check if the job has retries remaining."""
        return self.retry_count < self.max_retries

    def mark_retry(self) -> None:
        """Increment retry counter and reset status to pending."""
        self.retry_count += 1
        self.status = CrawlStatus.PENDING

    def mark_running(self) -> None:
        """Mark job as currently running."""
        self.status = CrawlStatus.RUNNING

    def mark_completed(self) -> None:
        """Mark job as successfully completed."""
        self.status = CrawlStatus.COMPLETED

    def mark_failed(self) -> None:
        """Mark job as failed."""
        self.status = CrawlStatus.FAILED


@dataclass
class PageContent:
    """
    Holds the raw and processed content retrieved from a web page.

    Attributes:
        url: The source URL.
        html: Raw HTML content.
        text: Cleaned plain-text content.
        metadata: Page metadata (title, description, headers, etc.).
        timestamp: When the content was fetched (epoch seconds).
        status_code: HTTP status code from the fetch.
    """
    url: str
    html: str = ""
    text: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    status_code: int = 200

    @property
    def content_hash(self) -> str:
        """SHA-256 hash of the text content for deduplication."""
        return hashlib.sha256(self.text.encode()).hexdigest()

    @property
    def word_count(self) -> int:
        """Number of words in the text content."""
        return len(self.text.split()) if self.text else 0

    def extract_title(self) -> str:
        """Extract the page title from HTML using a simple regex."""
        match = re.search(r"<title>(.*?)</title>", self.html, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else ""

    def extract_meta_description(self) -> str:
        """Extract meta description from HTML."""
        match = re.search(
            r'<meta\s+name=["\']description["\']\s+content=["\'](.*?)["\']',
            self.html,
            re.IGNORECASE,
        )
        return match.group(1).strip() if match else ""

    def extract_links(self) -> List[str]:
        """Extract all href links from HTML."""
        return re.findall(r'href=["\'](https?://[^"\']+)["\']', self.html)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize page content to a dictionary."""
        return {
            "url": self.url,
            "text": self.text,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "status_code": self.status_code,
            "word_count": self.word_count,
            "content_hash": self.content_hash,
        }


@dataclass
class ExtractionRule:
    """
    A named rule that matches a regex pattern against text and optionally
    transforms each match.

    Attributes:
        name: Human-readable rule name.
        pattern: Regex pattern string.
        transform: Optional callable applied to each raw match.
    """
    name: str
    pattern: str
    transform: Optional[Callable[[str], Any]] = None

    def apply(self, text: str) -> List[Any]:
        """Apply the rule to text, returning all matches (transformed if applicable)."""
        matches = re.findall(self.pattern, text)
        if self.transform is not None:
            return [self.transform(m) for m in matches]
        return matches

    def first_match(self, text: str) -> Optional[Any]:
        """Return only the first match, or None."""
        results = self.apply(text)
        return results[0] if results else None


class ContentExtractor:
    """
    Applies a collection of ExtractionRules to page text to produce
    structured output keyed by rule name.
    """

    def __init__(self) -> None:
        self._rules: List[ExtractionRule] = []

    def add_rule(self, rule: ExtractionRule) -> None:
        """Register an extraction rule."""
        self._rules.append(rule)

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name. Returns True if found and removed."""
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.name != name]
        return len(self._rules) < before

    @property
    def rules(self) -> List[ExtractionRule]:
        """List of registered rules."""
        return list(self._rules)

    def extract(self, text: str) -> Dict[str, List[Any]]:
        """Run all rules against text and return results keyed by rule name."""
        results: Dict[str, List[Any]] = {}
        for rule in self._rules:
            matches = rule.apply(text)
            if matches:
                results[rule.name] = matches
        return results

    def extract_first(self, text: str) -> Dict[str, Any]:
        """Run all rules but keep only the first match per rule."""
        results: Dict[str, Any] = {}
        for rule in self._rules:
            first = rule.first_match(text)
            if first is not None:
                results[rule.name] = first
        return results

    def extract_from_page(self, page: PageContent) -> Dict[str, List[Any]]:
        """Extract data from a PageContent object's text field."""
        return self.extract(page.text)


class CrawlQueue:
    """
    Priority queue for crawl jobs with built-in deduplication.
    Lower priority numbers are dequeued first.
    """

    def __init__(self) -> None:
        self._jobs: List[CrawlJob] = []
        self._seen_ids: set = set()

    def enqueue(self, job: CrawlJob, force: bool = False) -> bool:
        """
        Add a job to the queue. Returns False if the job is a duplicate.
        Pass force=True to bypass dedup (used for retries).
        """
        jid = job.job_id
        if not force and jid in self._seen_ids:
            return False
        self._seen_ids.add(jid)
        self._jobs.append(job)
        self._jobs.sort(key=lambda j: j.priority)
        return True

    def dequeue(self) -> Optional[CrawlJob]:
        """Remove and return the highest-priority job, or None if empty."""
        if not self._jobs:
            return None
        return self._jobs.pop(0)

    def peek(self) -> Optional[CrawlJob]:
        """Return the highest-priority job without removing it."""
        return self._jobs[0] if self._jobs else None

    @property
    def size(self) -> int:
        """Number of jobs in the queue."""
        return len(self._jobs)

    @property
    def is_empty(self) -> bool:
        """Whether the queue has no jobs."""
        return len(self._jobs) == 0

    def clear(self) -> None:
        """Remove all jobs and reset dedup tracking."""
        self._jobs.clear()
        self._seen_ids.clear()

    def contains(self, url: str) -> bool:
        """Check if any queued job targets the given URL."""
        return any(j.url == url for j in self._jobs)

    def get_jobs_by_status(self, status: CrawlStatus) -> List[CrawlJob]:
        """Return all jobs with the given status."""
        return [j for j in self._jobs if j.status == status]


class CrawlEngine:
    """
    Orchestrates crawling: manages the queue, processes jobs through
    extraction, and collects results.

    In this library-only implementation (no HTTP), callers supply
    PageContent directly via ``process_page``.
    """

    def __init__(self, max_concurrent: int = 5) -> None:
        self.queue = CrawlQueue()
        self.extractor = ContentExtractor()
        self.max_concurrent: int = max_concurrent
        self._results: Dict[str, Dict[str, Any]] = {}
        self._processed_urls: set = set()

    # -- rule management --------------------------------------------------

    def add_extraction_rule(self, rule: ExtractionRule) -> None:
        """Register an extraction rule with the engine's extractor."""
        self.extractor.add_rule(rule)

    # -- job management ---------------------------------------------------

    def submit_job(self, job: CrawlJob) -> bool:
        """Submit a crawl job to the queue."""
        return self.queue.enqueue(job)

    def submit_url(self, url: str, selectors: Optional[List[str]] = None,
                   priority: int = 5) -> bool:
        """Convenience: create and submit a job from a URL."""
        job = CrawlJob(url=url, selectors=selectors or [], priority=priority)
        return self.queue.enqueue(job)

    # -- processing -------------------------------------------------------

    def process_page(self, page: PageContent) -> Dict[str, Any]:
        """
        Run extraction on a PageContent and store the results.

        Returns the extraction results dict.
        """
        extracted = self.extractor.extract(page.text)
        result = {
            "url": page.url,
            "title": page.extract_title(),
            "word_count": page.word_count,
            "content_hash": page.content_hash,
            "extracted": extracted,
        }
        self._results[page.url] = result
        self._processed_urls.add(page.url)
        return result

    def process_next(self, page_content_map: Dict[str, PageContent]) -> Optional[Dict[str, Any]]:
        """
        Dequeue the next job, look up its PageContent from the provided map,
        and process it. Returns None when the queue is empty or the URL is
        missing from the map.
        """
        job = self.queue.dequeue()
        if job is None:
            return None
        job.mark_running()
        page = page_content_map.get(job.url)
        if page is None:
            job.mark_failed()
            return None
        result = self.process_page(page)
        job.mark_completed()
        return result

    # -- results ----------------------------------------------------------

    def get_result(self, url: str) -> Optional[Dict[str, Any]]:
        """Retrieve stored results for a URL."""
        return self._results.get(url)

    @property
    def results(self) -> Dict[str, Dict[str, Any]]:
        """All stored results."""
        return dict(self._results)

    @property
    def processed_count(self) -> int:
        """Number of URLs processed so far."""
        return len(self._processed_urls)

    def is_processed(self, url: str) -> bool:
        """Check if a URL has already been processed."""
        return url in self._processed_urls

    def reset(self) -> None:
        """Clear all results and queue state."""
        self._results.clear()
        self._processed_urls.clear()
        self.queue.clear()
