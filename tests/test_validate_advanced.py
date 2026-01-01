# tests/test_validate_advanced.py

import os
from unittest.mock import Mock
import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.qualitychecks import validate_data


def test_calculate_quality_score_perfect():
    results = {"nulls": {}, "duplicates": {}, "referential_integrity": {}, "ranges": {}}
    score = validate_data.calculate_quality_score(results)
    assert score == 100.0


def test_table_exists_mock():
    mock_conn = Mock()
    mock_conn.execute.return_value.scalar_one.return_value = True
    assert validate_data.table_exists(mock_conn, "staging.customers") is True


def test_calculate_quality_score_with_violations():
    check_results = {
        "nulls": {
            "staging.customers": {"email": 2, "firstname": 1},
        },
        "duplicates": {
            "staging.customers": {"duplicate_pk_count": 3},
        },
        "referential_integrity": {
            "total_orphans": 4,
        },
        "ranges": {
            "products_invalid_price_cost": 2,
            "transactionitems_invalid_ranges": 1,
        },
    }
    score = validate_data.calculate_quality_score(check_results)
    assert 0 <= score < 100  # penalties applied


def test_check_results_aggregation_and_report(monkeypatch, tmp_path):
    """Smoke-test main(): aggregation + JSON report written."""

    class DummyConn:
        def execute(self, *args, **kwargs):
            class R:
                def scalar_one(self_inner):
                    return 0
            return R()

    class DummyEngine:
        def connect(self):
            class Ctx:
                def __enter__(self_inner):
                    return DummyConn()
                def __exit__(self_inner, exc_type, exc, tb):
                    return False
            return Ctx()

    # Monkeypatch engine + config so main() does not hit real DB
    monkeypatch.setattr(validate_data, "get_engine", lambda config: DummyEngine())
    monkeypatch.setattr(validate_data, "load_config", lambda path=validate_data.CONFIG_PATH: {})

    # Ensure reports directory is under current working dir
    reports_dir = Path("reports")
    if reports_dir.exists():
        # Optional: clean existing file
        quality_report = reports_dir / "quality_report.json"
        if quality_report.exists():
            quality_report.unlink()

    validate_data.main()

    assert reports_dir.exists()
    assert (reports_dir / "quality_report.json").exists()


def test_check_warehouse_integrity_queries():
    """Cover check_warehouse_integrity by mocking connection."""

    class DummyResult:
        def __init__(self, value):
            self._value = value
        def scalar_one(self):
            return self._value

    class DummyConn:
        def __init__(self):
            self.calls = 0
        def execute(self, *args, **kwargs):
            self.calls += 1
            return DummyResult(self.calls)  # 1, then 2, then 3

    conn = DummyConn()
    res = validate_data.check_warehouse_integrity(conn)

    assert res["fact_customers_orphans"] == 1
    assert res["fact_orders_orphans"] == 2
    assert res["fact_date_orphans"] == 3
    assert res["total_orphans"] == 1 + 2 + 3


def test_check_warehouse_ranges_queries():
    """Cover check_warehouse_ranges by mocking connection."""

    class DummyResult:
        def __init__(self, value):
            self._value = value
        def scalar_one(self):
            return self._value

    class DummyConn:
        def __init__(self):
            self.calls = 0
        def execute(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return DummyResult(0)
            elif self.calls == 2:
                return DummyResult(5)
            else:
                return DummyResult(2)

    conn = DummyConn()
    res = validate_data.check_warehouse_ranges(conn)

    assert res["negative_quantity"] == 0
    assert res["negative_price"] == 5
    assert res["total_mismatch"] == 2


def test_main_with_warehouse_tables(monkeypatch, tmp_path):
    """Force table_exists=True so warehouse checks branch executes."""

    class DummyConn:
        def execute(self, *args, **kwargs):
            class R:
                def scalar_one(self_inner):
                    return 0
            return R()

    class DummyEngine:
        def connect(self):
            class Ctx:
                def __enter__(self_inner):
                    return DummyConn()
                def __exit__(self_inner, exc_type, exc, tb):
                    return False
            return Ctx()

    # Monkeypatch helpers
    monkeypatch.setattr(validate_data, "get_engine", lambda config: DummyEngine())
    monkeypatch.setattr(validate_data, "load_config", lambda path=validate_data.CONFIG_PATH: {})

    # Force table_exists to return True for warehouse.dimcustomers
    monkeypatch.setattr(validate_data, "table_exists", lambda conn, name: True)

    # Stub warehouse check functions
    monkeypatch.setattr(validate_data, "check_warehouse_integrity", lambda conn: {"total_orphans": 0})
    monkeypatch.setattr(validate_data, "check_warehouse_ranges", lambda conn: {"negative_quantity": 0})

    reports_dir = Path("reports")
    if reports_dir.exists():
        quality_report = reports_dir / "quality_report.json"
        if quality_report.exists():
            quality_report.unlink()

    validate_data.main()

    assert reports_dir.exists()
    assert (reports_dir / "quality_report.json").exists()
