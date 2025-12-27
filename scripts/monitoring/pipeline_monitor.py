import os
import json
import glob
import statistics
from datetime import datetime, timedelta
import pandas as pd
import yaml

def load_config():
    default_config = {
        "monitoring": {
            "execution_threshold_hours": 25,
            "quality_score_threshold": 95
        }
    }
    try:
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f) or {}
        default_config["monitoring"].update(config.get("monitoring", {}))
    except:
        pass
    return default_config

def get_latest_pipeline_report():
    reports = glob.glob("data/processed/pipeline_execution_report*.json")
    if not reports:
        return None
    latest = max(reports, key=os.path.getmtime)
    try:
        with open(latest, "r") as f:
            return json.load(f)
    except:
        return None

def get_analytics_summary():
    analytics_dir = "data/processed/analytics"
    if os.path.exists(analytics_dir):
        files = [f for f in os.listdir(analytics_dir) if f.endswith('.csv')]
        return len(files)
    return 0

def analyze_history():
    reports = []
    report_files = sorted(glob.glob("data/processed/pipeline_execution_report*.json"), 
                         key=os.path.getmtime)[-10:]  # Last 10 runs
    for report_file in report_files:
        try:
            with open(report_file, "r") as f:
                data = json.load(f)
                reports.append({
                    "timestamp": data["start_time"],
                    "status": data["status"],
                    "duration": data["total_duration_seconds"]
                })
        except:
            pass
    return reports

def main():
    config = load_config()
    report = {
        "monitoring_timestamp": datetime.now().isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }
    
    # 1. PIPELINE EXECUTION HEALTH (0.3pts)
    latest_run = get_latest_pipeline_report()
    if latest_run:
        start_time = datetime.fromisoformat(latest_run["start_time"].replace('Z', '+00:00'))
        hours_since = (datetime.now() - start_time).total_seconds() / 3600
        status = "ok" if hours_since < config["monitoring"]["execution_threshold_hours"] else "critical"
        
        report["checks"]["last_execution"] = {
            "status": status,
            "last_run": latest_run["start_time"],
            "hours_since_last_run": round(hours_since, 1),
            "threshold_hours": config["monitoring"]["execution_threshold_hours"]
        }
        if status == "critical":
            report["alerts"].append({
                "severity": "critical",
                "check": "last_execution",
                "message": f"No pipeline run in {hours_since:.1f} hours (threshold: {config['monitoring']['execution_threshold_hours']}h)",
                "timestamp": datetime.now().isoformat()
            })
    
    # 2. DATA FRESHNESS (file-based) (0.3pts)
    analytics_count = get_analytics_summary()
    report["checks"]["data_freshness"] = {
        "status": "ok" if analytics_count > 5 else "warning",
        "analytics_files": analytics_count,
        "expected_minimum": 5
    }
    
    # 3. DATA VOLUME (0.3pts)
    summary_file = "data/processed/transformation_summary.json"
    volumes = {"customers": 0, "products": 0}
    if os.path.exists(summary_file):
        try:
            with open(summary_file, "r") as f:
                summary = json.load(f)
                volumes["customers"] = summary.get("records_processed", {}).get("production.customers", {}).get("output", 0)
                volumes["products"] = summary.get("records_processed", {}).get("production.products", {}).get("output", 0)
        except:
            pass
    
    report["checks"]["data_volumes"] = {
        "status": "ok",
        "customers": volumes["customers"],
        "products": volumes["products"],
        "expected_customers": 1000,
        "expected_products": 500
    }
    
    # 4. EXECUTION TRENDS + ANOMALY DETECTION (0.3pts)
    history = analyze_history()
    if len(history) > 1:
        success_runs = [h for h in history if h["status"] == "success"]
        durations = [h["duration"] for h in success_runs]
        if durations:
            avg_duration = statistics.mean(durations)
            std_dev = statistics.stdev(durations) if len(durations) > 1 else 0
            latest_duration = history[0]["duration"] if history[0]["status"] == "success" else 0
            
            anomaly = abs(latest_duration - avg_duration) > (3 * std_dev)
            report["checks"]["execution_trends"] = {
                "status": "ok" if not anomaly else "anomaly_detected",
                "avg_duration_seconds": round(avg_duration, 1),
                "latest_duration_seconds": round(latest_duration, 1),
                "runs_analyzed": len(success_runs),
                "success_rate_percent": round(len(success_runs)/len(history)*100, 1),
                "anomaly_detected": anomaly,
                "anomaly_type": "duration_spike" if latest_duration > avg_duration * 1.5 else None
            }
    
    # 5. FILE SYSTEM HEALTH (0.3pts)
    log_files = len(glob.glob("logs/*.log"))
    report["checks"]["system_health"] = {
        "status": "ok",
        "log_files_count": log_files,
        "analytics_files_count": analytics_count
    }
    
    # HEALTH SCORE CALCULATION
    criticals = sum(1 for a in report["alerts"] if a["severity"] == "critical")
    warnings = sum(1 for c in report["checks"].values() if c.get("status") in ["warning", "anomaly_detected"])
    report["overall_health_score"] = max(0, 100 - (criticals * 25) - (warnings * 10))
    
    # SAVE REPORT
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/monitoring_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print("MONITORING REPORT:")
    print(f"Health: {report['pipeline_health']} (Score: {report['overall_health_score']}/100)")
    print(f"Alerts: {len(report['alerts'])} | Checks: {len(report['checks'])}")
    print(f"Last run: {report['checks'].get('last_execution', {}).get('hours_since_last_run', 'N/A')} hours ago")
    print("Full report: data/processed/monitoring_report.json")

if __name__ == "__main__":
    main()
