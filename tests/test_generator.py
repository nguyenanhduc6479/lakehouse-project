"""Unit tests for the pure helpers of source/generator.py (no database needed)."""

from __future__ import annotations

import random
import re
from decimal import Decimal

import pytest
from generator import (
    ACTIONS,
    PRODUCTS,
    TRANSITIONS,
    ascii_slug,
    build_order_lines,
    line_total,
    make_email,
    make_phone,
    next_status,
)

# must match the CHECK constraints in source/schema.sql
ORDER_STATUSES = {"pending", "paid", "shipped", "completed", "cancelled", "refunded"}


def test_line_total_applies_discount_and_rounds():
    assert line_total(2, Decimal("350000"), Decimal("50000")) == Decimal("650000.00")
    assert line_total(3, Decimal("0.333"), Decimal(0)) == Decimal("1.00")


def test_line_total_never_negative():
    assert line_total(1, Decimal("100"), Decimal("500")) == Decimal("0.00")


@pytest.mark.parametrize("status", sorted(TRANSITIONS))
def test_next_status_only_returns_valid_statuses(status):
    rng = random.Random(0)
    for _ in range(200):
        nxt = next_status(status, rng)
        assert nxt in ORDER_STATUSES
        assert nxt != status


@pytest.mark.parametrize("status", ["cancelled", "refunded"])
def test_final_statuses_have_no_next(status):
    assert next_status(status, random.Random(0)) is None


def test_all_transition_targets_are_schema_statuses():
    targets = {s for options in TRANSITIONS.values() for s, _ in options}
    assert targets | set(TRANSITIONS) <= ORDER_STATUSES


def test_build_order_lines_shape():
    rng = random.Random(42)
    skus = {p[0] for p in PRODUCTS}
    for _ in range(300):
        lines = build_order_lines(rng)
        assert 1 <= len(lines) <= 5
        assert [line["line_number"] for line in lines] == list(range(1, len(lines) + 1))
        assert len({line["sku"] for line in lines}) == len(lines)  # distinct products
        for line in lines:
            assert line["sku"] in skus
            assert line["quantity"] > 0
            assert line["discount_amount"] >= 0
            assert line["line_total"] == line_total(
                line["quantity"], line["unit_price"], line["discount_amount"]
            )


def test_ascii_slug_handles_vietnamese():
    assert ascii_slug("Nguyễn Văn Đức") == "nguyen.van.duc"
    assert ascii_slug("  Trần   Thị  Ánh ") == "tran.thi.anh"
    assert ascii_slug("!!!") == "user"


def test_make_email_and_phone_fit_the_schema():
    rng = random.Random(1)
    email = make_email("Lê Hoàng Phúc", rng)
    assert re.fullmatch(r"le\.hoang\.phuc\.\d{4}@example\.com", email)
    assert len(email) <= 255
    phone = make_phone(rng)
    assert re.fullmatch(r"0\d{9}", phone)  # varchar(20) in shop.users


def test_action_weights_are_positive():
    assert all(w > 0 for w in ACTIONS.values())
