"""Tests for matsya.core — CrawlJob, PageContent, ExtractionRule, ContentExtractor, CrawlQueue, CrawlEngine."""

import pytest
from matsya.core import (
    CrawlEngine,
    CrawlJob,
    CrawlQueue,
    CrawlStatus,
    ContentExtractor,
    ExtractionRule,
    OutputFormat,
    PageContent,
)


# -- CrawlJob -----------------------------------------------------------------

class TestCrawlJob:
    def test_default_values(self):
        job = CrawlJob(url="https://example.com")
        assert job.url == "https://example.com"
        assert job.selectors == []
        assert job.output_format == "json"
        assert job.priority == 5
        assert job.status == CrawlStatus.PENDING

    def test_job_id_deterministic(self):
        a = CrawlJob(url="https://a.com", selectors=["h1", "p"])
        b = CrawlJob(url="https://a.com", selectors=["p", "h1"])
        assert a.job_id == b.job_id  # sorted selectors

    def test_retry_lifecycle(self):
        job = CrawlJob(url="https://x.com", max_retries=2)
        assert job.can_retry()
        job.mark_retry()
        assert job.retry_count == 1
        assert job.status == CrawlStatus.PENDING
        job.mark_retry()
        assert not job.can_retry()

    def test_status_transitions(self):
        job = CrawlJob(url="https://x.com")
        job.mark_running()
        assert job.status == CrawlStatus.RUNNING
        job.mark_completed()
        assert job.status == CrawlStatus.COMPLETED

    def test_mark_failed(self):
        job = CrawlJob(url="https://x.com")
        job.mark_failed()
        assert job.status == CrawlStatus.FAILED


# -- PageContent ---------------------------------------------------------------

class TestPageContent:
    def test_word_count(self):
        page = PageContent(url="https://a.com", text="hello world foo")
        assert page.word_count == 3

    def test_content_hash_consistent(self):
        p1 = PageContent(url="https://a.com", text="same")
        p2 = PageContent(url="https://b.com", text="same")
        assert p1.content_hash == p2.content_hash

    def test_extract_title(self):
        html = "<html><head><title>My Page</title></head></html>"
        page = PageContent(url="https://a.com", html=html)
        assert page.extract_title() == "My Page"

    def test_extract_meta_description(self):
        html = '<meta name="description" content="A nice page">'
        page = PageContent(url="https://a.com", html=html)
        assert page.extract_meta_description() == "A nice page"

    def test_extract_links(self):
        html = '<a href="https://a.com/1">1</a> <a href="https://b.com/2">2</a>'
        page = PageContent(url="https://x.com", html=html)
        assert page.extract_links() == ["https://a.com/1", "https://b.com/2"]

    def test_to_dict(self):
        page = PageContent(url="https://a.com", text="hi")
        d = page.to_dict()
        assert d["url"] == "https://a.com"
        assert d["word_count"] == 1
        assert "content_hash" in d


# -- ExtractionRule & ContentExtractor -----------------------------------------

class TestExtractionRule:
    def test_basic_match(self):
        rule = ExtractionRule(name="numbers", pattern=r"\d+")
        assert rule.apply("abc 42 def 7") == ["42", "7"]

    def test_transform(self):
        rule = ExtractionRule(name="ints", pattern=r"\d+", transform=int)
        assert rule.apply("a1 b22") == [1, 22]

    def test_first_match(self):
        rule = ExtractionRule(name="words", pattern=r"[A-Z]\w+")
        assert rule.first_match("Hello World") == "Hello"

    def test_no_match(self):
        rule = ExtractionRule(name="x", pattern=r"zzz")
        assert rule.apply("abc") == []
        assert rule.first_match("abc") is None


class TestContentExtractor:
    def test_extract_multiple_rules(self):
        ext = ContentExtractor()
        ext.add_rule(ExtractionRule(name="emails", pattern=r"[\w.]+@[\w.]+"))
        ext.add_rule(ExtractionRule(name="nums", pattern=r"\d+"))
        result = ext.extract("Contact foo@bar.com or 123")
        assert "emails" in result
        assert "nums" in result

    def test_remove_rule(self):
        ext = ContentExtractor()
        ext.add_rule(ExtractionRule(name="a", pattern=r"a"))
        assert ext.remove_rule("a")
        assert not ext.remove_rule("a")
        assert len(ext.rules) == 0

    def test_extract_first(self):
        ext = ContentExtractor()
        ext.add_rule(ExtractionRule(name="d", pattern=r"\d+"))
        result = ext.extract_first("10 20 30")
        assert result["d"] == "10"

    def test_extract_from_page(self):
        ext = ContentExtractor()
        ext.add_rule(ExtractionRule(name="w", pattern=r"[A-Z]\w+"))
        page = PageContent(url="https://x.com", text="Hello World")
        result = ext.extract_from_page(page)
        assert result["w"] == ["Hello", "World"]


# -- CrawlQueue ---------------------------------------------------------------

class TestCrawlQueue:
    def test_enqueue_dequeue_priority(self):
        q = CrawlQueue()
        q.enqueue(CrawlJob(url="https://low.com", priority=10))
        q.enqueue(CrawlJob(url="https://high.com", priority=1))
        job = q.dequeue()
        assert job.url == "https://high.com"

    def test_dedup(self):
        q = CrawlQueue()
        j = CrawlJob(url="https://a.com")
        assert q.enqueue(j)
        assert not q.enqueue(CrawlJob(url="https://a.com"))
        assert q.size == 1

    def test_peek_does_not_remove(self):
        q = CrawlQueue()
        q.enqueue(CrawlJob(url="https://a.com"))
        assert q.peek() is not None
        assert q.size == 1

    def test_empty_operations(self):
        q = CrawlQueue()
        assert q.is_empty
        assert q.dequeue() is None
        assert q.peek() is None

    def test_contains(self):
        q = CrawlQueue()
        q.enqueue(CrawlJob(url="https://a.com"))
        assert q.contains("https://a.com")
        assert not q.contains("https://b.com")

    def test_clear(self):
        q = CrawlQueue()
        q.enqueue(CrawlJob(url="https://a.com"))
        q.clear()
        assert q.is_empty


# -- CrawlEngine --------------------------------------------------------------

class TestCrawlEngine:
    def test_process_page(self):
        engine = CrawlEngine()
        engine.add_extraction_rule(ExtractionRule(name="nums", pattern=r"\d+"))
        page = PageContent(url="https://a.com", text="got 42 items",
                           html="<title>Items</title>")
        result = engine.process_page(page)
        assert result["url"] == "https://a.com"
        assert result["title"] == "Items"
        assert "42" in result["extracted"]["nums"]
        assert engine.is_processed("https://a.com")

    def test_submit_and_process_next(self):
        engine = CrawlEngine()
        engine.submit_url("https://a.com")
        page = PageContent(url="https://a.com", text="data")
        result = engine.process_next({"https://a.com": page})
        assert result is not None

    def test_process_next_empty_queue(self):
        engine = CrawlEngine()
        assert engine.process_next({}) is None

    def test_reset(self):
        engine = CrawlEngine()
        engine.submit_url("https://a.com")
        page = PageContent(url="https://a.com", text="x")
        engine.process_page(page)
        engine.reset()
        assert engine.processed_count == 0
        assert engine.queue.is_empty
