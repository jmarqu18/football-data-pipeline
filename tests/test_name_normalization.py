"""Tests for name preparation shared by team and player resolution.

These moved out of test_entity_resolution.py when the helpers were extracted
into pipeline.name_normalization to break the import cycle with
pipeline.resolution_ledger. The assertions are unchanged.
"""

from __future__ import annotations

from pipeline.name_normalization import build_name_variants, decode_api_name, normalize_name

# ─────────────────────────────────────────────────────────────
# Test: normalize_name
# ─────────────────────────────────────────────────────────────


class TestNormalizeName:
    def test_strips_diacritics(self):
        assert normalize_name("Vinícius Júnior") == "vinicius junior"

    def test_strips_spanish_accents(self):
        assert normalize_name("Álvaro Morata") == "alvaro morata"

    def test_handles_n_tilde(self):
        assert normalize_name("Iñaki Williams") == "inaki williams"

    def test_handles_nordic_characters(self):
        assert normalize_name("Alexander Sørloth") == "alexander sorloth"

    def test_lowercases(self):
        assert normalize_name("JUDE BELLINGHAM") == "jude bellingham"

    def test_collapses_whitespace(self):
        assert normalize_name("  Pedro   González   López  ") == "pedro gonzalez lopez"

    def test_empty_string(self):
        assert normalize_name("") == ""

    def test_decodes_html_entities_before_comparison(self):
        """HTML entities from API-Football are decoded so matching works correctly."""
        assert normalize_name("E. Eto&apos;o Pineda") == "e. eto'o pineda"
        assert normalize_name("Marcelo &amp; Silva") == "marcelo & silva"


# ─────────────────────────────────────────────────────────────
# Test: decode_api_name
# ─────────────────────────────────────────────────────────────


class TestDecodeApiName:
    def test_decodes_apostrophe_entity(self):
        assert decode_api_name("E. Eto&apos;o Pineda") == "E. Eto'o Pineda"

    def test_decodes_amp_entity(self):
        assert decode_api_name("Marcelo &amp; Silva") == "Marcelo & Silva"

    def test_passthrough_clean_name(self):
        assert decode_api_name("Robert Lewandowski") == "Robert Lewandowski"

    def test_decodes_numeric_entities(self):
        assert decode_api_name("Cami&#243;n") == "Camión"

    def test_numeric_entity_decoded(self):
        assert decode_api_name("Eto&#39;o") == "Eto'o"


# ─────────────────────────────────────────────────────────────
# Test: build_name_variants
# ─────────────────────────────────────────────────────────────


class TestBuildNameVariants:
    def test_full_name_only(self):
        variants = build_name_variants("Jude Bellingham")
        assert "jude bellingham" in variants

    def test_with_firstname_lastname(self):
        variants = build_name_variants("Pedro González López", firstname="Pedro", lastname="González López")
        assert "pedro gonzalez lopez" in variants
        assert "pedro" in variants
        assert "gonzalez lopez" in variants

    def test_no_duplicates(self):
        variants = build_name_variants("Pedro González López", firstname="Pedro", lastname="González López")
        assert len(variants) == len(set(variants))

    def test_none_fields_handled(self):
        variants = build_name_variants("Test Player", firstname=None, lastname=None)
        assert variants == ["test player"]
