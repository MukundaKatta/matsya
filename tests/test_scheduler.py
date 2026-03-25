"""Tests for matsya.scheduler — CrawlScheduler, CrawlHistory."""

import pytest
from matsya.core import CrawlJob
from matsya.scheduler import CrawlHistory, CrawlScheduler


class TestCrawlHistory:
    def test_record_and_query(self):
        h = CrawlHistory()
        h.record("https://a.com", "completed", duration=0.5)
        h.record("https://b.com", "failed", error="timeout")
        assert h.total == 2
        assert len(h.successes()) == 1
        assert len(h.failures()) == 1

    def test_for_url(self):
        h = CrawlHistory()
        h.record("https://a.com", "completed")
        h.record("https://a.com", "failed")
        assert len(h.for_url("https://a.com")) == 2

    def test_clear(self):
        h = CrawlHistory()
        h.record("https://a.com", "completed")
        h.clear()
        assert h.total == 0


class TestCrawlScheduler:
    def test_schedule_and_next(self):
        s = CrawlScheduler()
        s.schedule_url("https://a.com")
        job = s.next_job()
        assert job is not None
        assert job.url == "https://a.com"

    def test_dedup_via_visited(self):
        s = CrawlScheduler()
        s.schedule_url("https://a.com")
        job = s.next_job()
        s.complete_job(job)
        assert not s.schedule_url("https://a.com")

    def test_fail_and_retry(self):
        s = CrawlScheduler(max_retries=2)
        s.schedule_url("https://a.com")
        job = s.next_job()
        retried = s.fail_job(job, error="500")
        assert retried is True
        assert s.pending_count == 1

    def test_fail_exhausted_retries(self):
        s = CrawlScheduler(max_retries=1)
        s.schedule_url("https://a.com")
        job = s.next_job()
        s.fail_job(job, error="500")  # retry once
        job2 = s.next_job()
        retried = s.fail_job(job2, error="500")
        assert retried is False

    def test_rate_limit_check(self):
        s = CrawlScheduler(rate_limit_delay=100.0)
        s.record_access("https://a.com/page1")
        assert not s.can_fetch("https://a.com/page2")

    def test_reset(self):
        s = CrawlScheduler()
        s.schedule_url("https://a.com")
        s.reset()
        assert s.pending_count == 0
        assert s.visited_count == 0
