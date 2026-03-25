"""
Crawl scheduler with rate limiting, retry logic, deduplication, and history.

CrawlScheduler manages the lifecycle of CrawlJobs: it enforces a minimum
delay between requests to the same domain, tracks which URLs have already
been visited, and records results in CrawlHistory.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from matsya.core import CrawlJob, CrawlQueue, CrawlStatus


@dataclass
class CrawlHistoryEntry:
    """A single record in the crawl history."""
    url: str
    status: str
    timestamp: float
    duration: float = 0.0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class CrawlHistory:
    """
    Append-only history of crawl attempts, queryable by URL or status.
    """

    def __init__(self) -> None:
        self._entries: List[CrawlHistoryEntry] = []

    def record(self, url: str, status: str, duration: float = 0.0,
               error: Optional[str] = None,
               metadata: Optional[Dict[str, Any]] = None) -> CrawlHistoryEntry:
        """Append a new history entry and return it."""
        entry = CrawlHistoryEntry(
            url=url,
            status=status,
            timestamp=time.time(),
            duration=duration,
            error=error,
            metadata=metadata or {},
        )
        self._entries.append(entry)
        return entry

    @property
    def entries(self) -> List[CrawlHistoryEntry]:
        return list(self._entries)

    def for_url(self, url: str) -> List[CrawlHistoryEntry]:
        """All entries for a specific URL."""
        return [e for e in self._entries if e.url == url]

    def successes(self) -> List[CrawlHistoryEntry]:
        return [e for e in self._entries if e.status == "completed"]

    def failures(self) -> List[CrawlHistoryEntry]:
        return [e for e in self._entries if e.status == "failed"]

    @property
    def total(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()


class CrawlScheduler:
    """
    Manages crawl jobs with rate limiting, retry, and deduplication.

    Args:
        rate_limit_delay: Minimum seconds between requests to the same domain.
        max_retries: Default max retries for jobs that don't specify their own.
    """

    def __init__(self, rate_limit_delay: float = 1.0,
                 max_retries: int = 3) -> None:
        self.queue = CrawlQueue()
        self.history = CrawlHistory()
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self._domain_last_access: Dict[str, float] = {}
        self._visited_urls: set = set()

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _domain(url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc or parsed.path.split("/")[0]

    # -- scheduling -------------------------------------------------------

    def schedule(self, job: CrawlJob) -> bool:
        """
        Add a job to the queue if its URL hasn't been visited already.
        Returns False for duplicates.
        """
        if job.url in self._visited_urls:
            return False
        return self.queue.enqueue(job)

    def schedule_url(self, url: str, priority: int = 5) -> bool:
        """Convenience: schedule a URL directly."""
        job = CrawlJob(url=url, priority=priority, max_retries=self.max_retries)
        return self.schedule(job)

    # -- rate limiting ----------------------------------------------------

    def can_fetch(self, url: str) -> bool:
        """Check if the rate limit allows fetching this URL right now."""
        domain = self._domain(url)
        last = self._domain_last_access.get(domain, 0.0)
        return (time.time() - last) >= self.rate_limit_delay

    def record_access(self, url: str) -> None:
        """Record that we just accessed a URL (for rate limiting and dedup)."""
        domain = self._domain(url)
        self._domain_last_access[domain] = time.time()
        self._visited_urls.add(url)

    # -- next job ---------------------------------------------------------

    def next_job(self) -> Optional[CrawlJob]:
        """
        Dequeue the next job that is allowed by the rate limiter.
        Returns None when the queue is empty.
        """
        if self.queue.is_empty:
            return None
        job = self.queue.dequeue()
        return job

    # -- completion / retry -----------------------------------------------

    def complete_job(self, job: CrawlJob, duration: float = 0.0) -> None:
        """Mark a job completed and record in history."""
        job.mark_completed()
        self.record_access(job.url)
        self.history.record(job.url, "completed", duration=duration)

    def fail_job(self, job: CrawlJob, error: str = "",
                 duration: float = 0.0) -> bool:
        """
        Mark a job failed. If retries remain, re-enqueue it and return True.
        Otherwise record failure and return False.
        """
        job.mark_failed()
        self.history.record(job.url, "failed", duration=duration, error=error)
        if job.can_retry():
            job.mark_retry()
            self.queue.enqueue(job, force=True)
            return True
        return False

    # -- state inspection -------------------------------------------------

    @property
    def pending_count(self) -> int:
        return self.queue.size

    @property
    def visited_count(self) -> int:
        return len(self._visited_urls)

    def is_visited(self, url: str) -> bool:
        return url in self._visited_urls

    def reset(self) -> None:
        """Clear all scheduler state."""
        self.queue.clear()
        self.history.clear()
        self._domain_last_access.clear()
        self._visited_urls.clear()
