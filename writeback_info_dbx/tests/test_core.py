"""
Unit tests for the warehouse-agnostic core (src/core.py).

These cover the pure logic with no Spark, no Databricks, and no third-party
deps — runnable with plain `pytest` (or `python -m pytest`) on any machine.
The Sigma REST client (build_session/get_sigma_token/sigma_paginate) needs
`requests` and is exercised with a fake session object, so no network either.

This is the unit tier the test-rig issue (sigma_sandpit-a1r) builds on.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import core  # noqa: E402


# --- bar ---------------------------------------------------------------------

def test_bar_empty_total():
    assert core.bar(0, 0) == "0/0"


def test_bar_partial_and_full():
    assert core.bar(3, 5).endswith("3/5")
    full = core.bar(5, 5, width=4)
    assert "░" not in full and full.endswith("5/5")


# --- is_legacy_wal -----------------------------------------------------------

def test_is_legacy_wal():
    assert core.is_legacy_wal("`cat`.`sch`.`sigds_wal_abc123`") is True      # old uuid naming
    assert core.is_legacy_wal("`cat`.`sch`.`SIGDS_WAL_DS_42`") is False      # ds_id naming (case-insensitive)
    assert core.is_legacy_wal(None) is True                                  # missing -> treated legacy


# --- dedup_latest_by_edit_num ------------------------------------------------

def test_dedup_keeps_highest_edit_num():
    recs = [
        {"SIGDS_TABLE": "t1", "WAL_MAX_EDIT_NUM": 5},
        {"SIGDS_TABLE": "t1", "WAL_MAX_EDIT_NUM": 9},   # wins
        {"SIGDS_TABLE": "t2", "WAL_MAX_EDIT_NUM": 1},
        {"SIGDS_TABLE": None,  "WAL_MAX_EDIT_NUM": 99},  # skipped (no key)
    ]
    out = {r["SIGDS_TABLE"]: r["WAL_MAX_EDIT_NUM"] for r in core.dedup_latest_by_edit_num(recs)}
    assert out == {"t1": 9, "t2": 1}


def test_dedup_handles_none_edit_num():
    recs = [
        {"SIGDS_TABLE": "t1", "WAL_MAX_EDIT_NUM": None},
        {"SIGDS_TABLE": "t1", "WAL_MAX_EDIT_NUM": 0},
    ]
    # neither is > the other once coalesced to 0; first seen stays
    assert len(core.dedup_latest_by_edit_num(recs)) == 1


# --- select_enrichment -------------------------------------------------------

def test_select_enrichment_prefers_fresh_then_cache_then_empty():
    wb_meta = {"wb1": {"WORKBOOK_NAME": "fresh"}}
    cache   = {"wb1": {"WORKBOOK_NAME": "stale"}, "wb2": {"WORKBOOK_NAME": "cached"}}
    assert core.select_enrichment("wb1", wb_meta, cache)["WORKBOOK_NAME"] == "fresh"
    assert core.select_enrichment("wb2", wb_meta, cache)["WORKBOOK_NAME"] == "cached"
    assert core.select_enrichment("wb3", wb_meta, cache) == {}
    assert core.select_enrichment(None, wb_meta, cache) == {}


# --- build_id_index ----------------------------------------------------------

def test_build_id_index_picks_best_overlapping_key():
    # 'workbookId' overlaps the targets; 'ownerId' does not — best_key must be workbookId
    entries = [
        {"workbookId": "AbC", "ownerId": "zzz", "name": "one"},
        {"workbookId": "DeF", "ownerId": "yyy", "name": "two"},
    ]
    idx = core.build_id_index(entries, {"abc", "def"})
    assert set(idx.keys()) == {"abc", "def"}
    assert idx["abc"]["name"] == "one"


def test_build_id_index_empty_inputs():
    assert core.build_id_index([], {"x"}) == {}
    assert core.build_id_index([{"id": "a"}], set()) == {}


# --- sigma_paginate (fake session, no network) -------------------------------

class _FakeResp:
    def __init__(self, payload):
        self._payload = payload
    def raise_for_status(self):
        pass
    def json(self):
        return self._payload


class _FakeSession:
    """Returns queued pages in order; records the params it was called with."""
    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []
    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append(dict(params or {}))
        return _FakeResp(self._pages.pop(0))


def test_sigma_paginate_follows_next_page_and_flattens():
    sess = _FakeSession([
        {"entries": [1, 2], "nextPage": "p2"},
        {"entries": [3],    "nextPage": None},
    ])
    items = core.sigma_paginate(sess, "https://api/v2", "tok", "workbooks")
    assert items == [1, 2, 3]
    # second request carried the page cursor
    assert sess.calls[1].get("page") == "p2"


def test_sigma_paginate_alternate_root_key():
    sess = _FakeSession([{"workbooks": [{"id": "a"}], "nextPage": None}])
    assert core.sigma_paginate(sess, "https://api/v2", "tok", "workbooks") == [{"id": "a"}]
