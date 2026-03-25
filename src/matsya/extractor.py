"""
Structured data extractors for tables, lists, key-value pairs, and entities.

These extractors work on plain text (not HTML) and use regex-based heuristics
to identify common data structures embedded in textual content.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class TableExtractor:
    """
    Extracts tabular data from text that uses delimiter-separated rows.
    Handles pipe-delimited, tab-delimited, and comma-delimited tables.
    """

    DELIMITERS = ["|", "\t", ","]

    def extract(self, text: str, delimiter: Optional[str] = None) -> List[List[str]]:
        """
        Parse text into a list of rows, each row a list of cell strings.
        Auto-detects delimiter if not specified.
        """
        if delimiter is None:
            delimiter = self._detect_delimiter(text)
        rows: List[List[str]] = []
        for line in text.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            # skip markdown separator lines like |---|---|
            if re.fullmatch(r"[\|\-\s:]+", line):
                continue
            cells = [c.strip() for c in line.split(delimiter)]
            # strip leading/trailing empty cells from pipe tables
            if delimiter == "|" and cells and cells[0] == "":
                cells = cells[1:]
            if delimiter == "|" and cells and cells[-1] == "":
                cells = cells[:-1]
            if any(c for c in cells):
                rows.append(cells)
        return rows

    def extract_as_dicts(self, text: str, delimiter: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Extract a table and return rows as dicts keyed by the header row.
        """
        rows = self.extract(text, delimiter)
        if len(rows) < 2:
            return []
        headers = rows[0]
        result: List[Dict[str, str]] = []
        for row in rows[1:]:
            entry: Dict[str, str] = {}
            for i, header in enumerate(headers):
                entry[header] = row[i] if i < len(row) else ""
            result.append(entry)
        return result

    def _detect_delimiter(self, text: str) -> str:
        """Pick the delimiter that produces the most consistent column count."""
        best_delim = ","
        best_score = -1
        for delim in self.DELIMITERS:
            counts = []
            for line in text.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                if re.fullmatch(r"[\|\-\s:]+", line):
                    continue
                counts.append(line.count(delim))
            if not counts:
                continue
            # score: average count when count > 0, penalise variance
            nonzero = [c for c in counts if c > 0]
            if not nonzero:
                continue
            avg = sum(nonzero) / len(nonzero)
            variance = sum((c - avg) ** 2 for c in nonzero) / len(nonzero)
            score = avg / (1 + variance)
            if score > best_score:
                best_score = score
                best_delim = delim
        return best_delim


class ListExtractor:
    """
    Extracts ordered and unordered lists from plain text.
    Recognises bullets (-, *, +) and numbered items (1., 2., etc.).
    """

    BULLET_RE = re.compile(r"^\s*[-*+]\s+(.+)$")
    NUMBERED_RE = re.compile(r"^\s*\d+[.)]\s+(.+)$")

    def extract_bullets(self, text: str) -> List[str]:
        """Return items from bullet lists."""
        items: List[str] = []
        for line in text.splitlines():
            m = self.BULLET_RE.match(line)
            if m:
                items.append(m.group(1).strip())
        return items

    def extract_numbered(self, text: str) -> List[str]:
        """Return items from numbered lists."""
        items: List[str] = []
        for line in text.splitlines():
            m = self.NUMBERED_RE.match(line)
            if m:
                items.append(m.group(1).strip())
        return items

    def extract_all(self, text: str) -> Dict[str, List[str]]:
        """Extract both bullet and numbered lists."""
        return {
            "bullets": self.extract_bullets(text),
            "numbered": self.extract_numbered(text),
        }


class EntityExtractor:
    """
    Extracts common entity types: email addresses, phone numbers, dates,
    URLs, and currency amounts from text.
    """

    EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    PHONE_RE = re.compile(
        r"(?:\+?\d{1,3}[\s\-]?)?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4}"
    )
    DATE_RE = re.compile(
        r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b"
        r"|\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b"
        r"|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b"
    )
    URL_RE = re.compile(r"https?://[^\s\"'<>]+")
    CURRENCY_RE = re.compile(r"[\$\u00a3\u20ac]\s?\d[\d,]*\.?\d*")

    def extract_emails(self, text: str) -> List[str]:
        return self.EMAIL_RE.findall(text)

    def extract_phones(self, text: str) -> List[str]:
        raw = self.PHONE_RE.findall(text)
        # filter out things that are too short to be phone numbers
        return [p.strip() for p in raw if sum(c.isdigit() for c in p) >= 7]

    def extract_dates(self, text: str) -> List[str]:
        return self.DATE_RE.findall(text)

    def extract_urls(self, text: str) -> List[str]:
        return self.URL_RE.findall(text)

    def extract_currencies(self, text: str) -> List[str]:
        return self.CURRENCY_RE.findall(text)

    def extract_all(self, text: str) -> Dict[str, List[str]]:
        """Run all entity extractors and return a combined dict."""
        return {
            "emails": self.extract_emails(text),
            "phones": self.extract_phones(text),
            "dates": self.extract_dates(text),
            "urls": self.extract_urls(text),
            "currencies": self.extract_currencies(text),
        }


class StructuredExtractor:
    """
    Facade that combines table, list, and entity extraction into a single
    call, returning a unified structured-data dictionary.
    """

    def __init__(self) -> None:
        self.table_extractor = TableExtractor()
        self.list_extractor = ListExtractor()
        self.entity_extractor = EntityExtractor()

    def extract(self, text: str) -> Dict[str, Any]:
        """Run all sub-extractors and merge results."""
        return {
            "tables": self.table_extractor.extract(text),
            "lists": self.list_extractor.extract_all(text),
            "entities": self.entity_extractor.extract_all(text),
        }

    def extract_tables(self, text: str, delimiter: Optional[str] = None) -> List[List[str]]:
        return self.table_extractor.extract(text, delimiter)

    def extract_lists(self, text: str) -> Dict[str, List[str]]:
        return self.list_extractor.extract_all(text)

    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        return self.entity_extractor.extract_all(text)
