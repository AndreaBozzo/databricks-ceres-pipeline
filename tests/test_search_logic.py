"""
Unit tests for semantic search logic.

Tests the sparse dot product computation and text_soup construction
using pure Python equivalents (no Spark required).
"""

import re

import pytest

# ---------------------------------------------------------------------------
# Helpers — mirror the search logic in pure Python
# ---------------------------------------------------------------------------


def sparse_dot_product(doc_indices, doc_values, query_indices, query_values):
    """Pure Python equivalent of the dot product logic used in the search engine."""
    try:
        doc_map = dict(zip(doc_indices, doc_values, strict=False))
        score = 0.0
        for qi, qv in zip(query_indices, query_values, strict=False):
            if qi in doc_map:
                score += doc_map[qi] * qv
        return float(score)
    except (TypeError, ValueError):
        return None


def build_text_soup(title, tags, description, title_weight=2):
    """Pure Python equivalent of the text_soup construction in 04_semantic_search_engine.py."""
    parts = [title] * title_weight + [tags, description]
    text = " ".join(p for p in parts if p)
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    return text


# ---------------------------------------------------------------------------
# Tests — Sparse Dot Product
# ---------------------------------------------------------------------------


class TestSparseDotProduct:
    def test_identical_vectors(self):
        indices = [0, 2, 5]
        values = [1.0, 2.0, 3.0]
        result = sparse_dot_product(indices, values, indices, values)
        assert result == pytest.approx(14.0)  # 1+4+9

    def test_orthogonal_vectors(self):
        """Vectors with no shared indices should return 0."""
        result = sparse_dot_product([0, 1], [1.0, 1.0], [2, 3], [1.0, 1.0])
        assert result == pytest.approx(0.0)

    def test_partial_overlap(self):
        """Only overlapping indices contribute to the score."""
        doc = ([0, 1, 2], [1.0, 2.0, 3.0])
        query = ([1, 2, 3], [0.5, 0.5, 0.5])
        result = sparse_dot_product(*doc, *query)
        # overlap at 1: 2.0*0.5=1.0, overlap at 2: 3.0*0.5=1.5
        assert result == pytest.approx(2.5)

    def test_empty_query(self):
        result = sparse_dot_product([0, 1], [1.0, 2.0], [], [])
        assert result == pytest.approx(0.0)

    def test_empty_document(self):
        result = sparse_dot_product([], [], [0, 1], [1.0, 2.0])
        assert result == pytest.approx(0.0)

    def test_both_empty(self):
        result = sparse_dot_product([], [], [], [])
        assert result == pytest.approx(0.0)

    def test_none_input_returns_none(self):
        result = sparse_dot_product(None, None, [0], [1.0])
        assert result is None

    def test_single_element(self):
        result = sparse_dot_product([5], [3.0], [5], [4.0])
        assert result == pytest.approx(12.0)

    def test_float_precision(self):
        """Verify no floating point drift on realistic values."""
        result = sparse_dot_product([0], [0.1], [0], [0.2])
        assert result == pytest.approx(0.02, abs=1e-9)


# ---------------------------------------------------------------------------
# Tests — Text Soup Construction
# ---------------------------------------------------------------------------


class TestTextSoup:
    def test_basic_construction(self):
        result = build_text_soup("My Title", "tag1, tag2", "A description")
        assert "my title" in result
        assert "tag1" in result
        assert "a description" in result

    def test_title_repeated_by_weight(self):
        result = build_text_soup("important", "tag", "desc", title_weight=3)
        assert result.count("important") == 3

    def test_title_weight_one(self):
        result = build_text_soup("title", "tag", "desc", title_weight=1)
        assert result.count("title") == 1

    def test_special_characters_removed(self):
        result = build_text_soup("Hello-World!", "c++, c#", "test@email.com")
        assert "-" not in result
        assert "!" not in result
        assert "+" not in result
        assert "#" not in result
        assert "@" not in result

    def test_lowercased(self):
        result = build_text_soup("UPPER Case", "TAG", "DESC")
        assert result == result.lower()

    def test_empty_fields(self):
        result = build_text_soup("title", "", "")
        assert "title" in result

    def test_none_fields_handled(self):
        result = build_text_soup("title", None, None)
        assert "title" in result
