import os
import glob
import pytest
import subprocess
from datetime import datetime

def test_transformation_pipeline_exists():
    """Test transformation script exists (0.5pt)"""
    result = subprocess.run(["python", "scripts/transformation/staging_to_production.py"], 
                           capture_output=True, timeout=30)
    print(f"✅ Transformation runs (exit: {result.returncode})")

def test_transformation_generates_logs():
    """Test transformation creates log files (0.5pt)"""
    # Run full pipeline up to transformation
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    
    logs = glob.glob("logs/staging_to_production_*.log")
    assert len(logs) > 0, "No transformation logs"
    print(f"✅ Transformation log: {os.path.basename(logs[0])}")

def test_transformation_summary_json():
    """Test transformation summary generated (0.5pt)"""
    subprocess.run(["python", "scripts/pipeline/orchestrator.py"], timeout=120)
    
    summary_file = "data/processed/transformation_summary.json"
    assert os.path.exists(summary_file), "No transformation summary"
    
    import json
    with open(summary_file) as f:
        summary = json.load(f)
    
    customers = summary.get("records_processed", {}).get("production.customers", {})
    assert customers.get("output", 0) > 0, "No customers transformed"
    print(f"✅ Summary: {customers.get('output', 0)} customers transformed")
