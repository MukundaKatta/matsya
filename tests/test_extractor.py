"""Tests for matsya.extractor — TableExtractor, ListExtractor, EntityExtractor, StructuredExtractor."""

import pytest
from matsya.extractor import (
    EntityExtractor,
    ListExtractor,
    StructuredExtractor,
    TableExtractor,
)


class TestTableExtractor:
    def test_pipe_table(self):
        text = (
            "| Name  | Age |\n"
            "|-------|-----|\n"
            "| Alice | 30  |\n"
            "| Bob   | 25  |"
        )
        rows = TableExtractor().extract(text, delimiter="|")
        assert len(rows) == 3  # header + 2 data rows
        assert rows[0] == ["Name", "Age"]

    def test_csv_table(self):
        text = "name,score\nAlice,95\nBob,87"
        rows = TableExtractor().extract(text, delimiter=",")
        assert len(rows) == 3
        assert rows[1] == ["Alice", "95"]

    def test_extract_as_dicts(self):
        text = "Name|Age\nAlice|30\nBob|25"
        dicts = TableExtractor().extract_as_dicts(text, delimiter="|")
        assert len(dicts) == 2
        assert dicts[0]["Name"] == "Alice"
        assert dicts[1]["Age"] == "25"

    def test_auto_detect_delimiter(self):
        text = "a\tb\n1\t2\n3\t4"
        rows = TableExtractor().extract(text)
        assert len(rows) == 3


class TestListExtractor:
    def test_bullet_list(self):
        text = "- apple\n- banana\n- cherry"
        items = ListExtractor().extract_bullets(text)
        assert items == ["apple", "banana", "cherry"]

    def test_numbered_list(self):
        text = "1. first\n2. second\n3. third"
        items = ListExtractor().extract_numbered(text)
        assert items == ["first", "second", "third"]

    def test_extract_all(self):
        text = "- bullet\n1. numbered"
        result = ListExtractor().extract_all(text)
        assert len(result["bullets"]) == 1
        assert len(result["numbered"]) == 1

    def test_mixed_bullets(self):
        text = "* star\n+ plus\n- dash"
        items = ListExtractor().extract_bullets(text)
        assert len(items) == 3


class TestEntityExtractor:
    def test_emails(self):
        text = "Contact alice@example.com or bob@test.org"
        emails = EntityExtractor().extract_emails(text)
        assert "alice@example.com" in emails
        assert "bob@test.org" in emails

    def test_phones(self):
        text = "Call 555-123-4567 or +1 (800) 555-0199"
        phones = EntityExtractor().extract_phones(text)
        assert len(phones) >= 1

    def test_dates(self):
        text = "Dates: 2024-01-15, March 5, 2024, 01/15/2024"
        dates = EntityExtractor().extract_dates(text)
        assert len(dates) >= 2

    def test_urls(self):
        text = "Visit https://example.com and http://test.org/page"
        urls = EntityExtractor().extract_urls(text)
        assert len(urls) == 2

    def test_currencies(self):
        text = "Price: $19.99 and $1,200.50"
        currencies = EntityExtractor().extract_currencies(text)
        assert len(currencies) == 2

    def test_extract_all(self):
        text = "Email alice@test.com, visit https://test.com, cost $5.00"
        result = EntityExtractor().extract_all(text)
        assert len(result["emails"]) == 1
        assert len(result["urls"]) == 1
        assert len(result["currencies"]) == 1


class TestStructuredExtractor:
    def test_facade_extract(self):
        text = (
            "- item1\n- item2\n"
            "Contact: alice@test.com\n"
            "a,b\n1,2"
        )
        result = StructuredExtractor().extract(text)
        assert "tables" in result
        assert "lists" in result
        assert "entities" in result
        assert "alice@test.com" in result["entities"]["emails"]

    def test_delegated_methods(self):
        se = StructuredExtractor()
        lists = se.extract_lists("- a\n- b")
        assert len(lists["bullets"]) == 2
        entities = se.extract_entities("foo@bar.com")
        assert len(entities["emails"]) == 1
