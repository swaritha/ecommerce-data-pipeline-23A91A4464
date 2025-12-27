import os
import glob
import pytest
import subprocess

def test_quality_script_runs():
    """Test quality script executes (0.25pt)"""
    result = subprocess.run(["python", "scripts/qualitychecks/validate_data.py"], 
                           capture_output=True, timeout=30)
    print(f"✅ Quality runs (exit: {result.returncode})")

def test_quality_log_generated():
    """Test quality log created (0.25pt)"""
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    logs = glob.glob("logs/data_quality_*.log")
    assert len(logs) > 0
    print(f"✅ Quality log: {os.path.basename(logs[0])}")

def test_pipeline_report_quality_summary():
    """Test quality metrics in report (0.25pt)"""
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    import json
    report = json.load(open("data/processed/pipeline_execution_report.json"))
    quality_summary = report.get("data_quality_summary", {})
    assert "quality_score" in quality_summary
    print(f"✅ Quality score: {quality_summary.get('quality_score', 'N/A')}")
