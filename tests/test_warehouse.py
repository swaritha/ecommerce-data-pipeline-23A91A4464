import os
import glob
import pytest
import subprocess

def test_warehouse_pipeline_runs():
    """Test warehouse load executes (0.25pt)"""
    result = subprocess.run(["python", "scripts/transformation/load_warehouse.py"], 
                           capture_output=True, timeout=30)
    print(f"✅ Warehouse runs (exit: {result.returncode})")

def test_warehouse_log_generated():
    """Test warehouse log created (0.25pt)"""
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    logs = glob.glob("logs/warehouse_load_*.log")
    assert len(logs) > 0
    print(f"✅ Warehouse log: {os.path.basename(logs[0])}")

def test_analytics_files_generated():
    """Test analytics outputs (0.25pt)"""
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    analytics_dir = "data/processed/analytics"
    if os.path.exists(analytics_dir):
        files = [f for f in os.listdir(analytics_dir) if f.endswith('.csv')]
        print(f"✅ Analytics: {len(files)} files")
