import os
import time
import json
import subprocess
import logging
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, "logs")
REPORT_PATH = os.path.join(BASE_DIR, "data", "processed", "pipeline_execution_report.json")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

# FIXED LOGGING - NO EMOJIS
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
main_log = os.path.join(LOG_DIR, f"pipeline_orchestrator_{timestamp}.log")
error_log = os.path.join(LOG_DIR, "pipeline_errors.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(main_log),
        logging.StreamHandler()
    ]
)

# Error-only logger
error_logger = logging.getLogger('errors')
error_handler = logging.FileHandler(error_log)
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

STEPS = [
    ("data_generation", ["python", "scripts/datageneration/generatedata.py"]),
    ("ingestion_staging", ["python", "scripts/ingestion/ingest_to_staging.py"]),
    ("data_quality", ["python", "scripts/qualitychecks/validate_data.py"]),
    ("staging_to_production", ["python", "scripts/transformation/staging_to_production.py"]),
    ("warehouse_load", ["python", "scripts/transformation/load_warehouse.py"]),
    ("analytics_generation", ["python", "scripts/transformation/generate_analytics.py"]),
]

def execute_step(step_name, command, max_retries=3):
    for attempt in range(max_retries + 1):
        try:
            logging.info(f"[RUN] Starting {step_name} (attempt {attempt + 1}/{max_retries + 1})")
            start_time = time.time()
            
            log_file = os.path.join(
                LOG_DIR,
                f"{step_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )
            
            with open(log_file, "w", encoding="utf-8") as lf:
                lf.write(f"[{datetime.now().isoformat()}] START {step_name}\n")
                
                result = subprocess.run(
                    command,
                    cwd=BASE_DIR,
                    stdout=lf,
                    stderr=lf,
                    timeout=120,
                    text=True
                )
                
                duration = time.time() - start_time
                
                if result.returncode == 0:
                    lf.write(f"[{datetime.now().isoformat()}] END {step_name} status=success duration={duration:.2f}s\n")
                    logging.info(f"[OK]  {step_name} SUCCESS ({duration:.1f}s)")
                    return {
                        "step_name": step_name,
                        "command": " ".join(command),
                        "status": "success",
                        "duration_seconds": round(duration, 2),
                        "log_file": os.path.relpath(log_file, BASE_DIR),
                        "error": "",
                        "retry_attempts": attempt
                    }
                else:
                    error_msg = f"Exit code {result.returncode}"
                    raise subprocess.CalledProcessError(result.returncode, command, error_msg)
                    
        except subprocess.TimeoutExpired:
            error_msg = f"{step_name} TIMEOUT after 120s"
            logging.warning(f"[WARN] {error_msg}")
            error_logger.error(error_msg)
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"[FAIL] {step_name} attempt {attempt + 1}: {error_msg}")
            error_logger.error(f"{step_name}: {error_msg}")
            
        if attempt < max_retries:
            wait_time = 2 ** attempt
            logging.info(f"[RETRY] {step_name} in {wait_time}s (backoff {attempt+1})")
            time.sleep(wait_time)
        else:
            return {
                "step_name": step_name,
                "command": " ".join(command),
                "status": "failed",
                "duration_seconds": round(time.time() - start_time, 2),
                "log_file": os.path.relpath(log_file, BASE_DIR),
                "error": error_msg,
                "retry_attempts": max_retries
            }

def main():
    pipeline_start = datetime.now()
    results = []
    overall_status = "success"
    
    logging.info("PIPELINE EXECUTION STARTED")
    
    for name, cmd in STEPS:
        logging.info(f"[STEP] Executing {len(results)+1}/6: {name}")
        step_result = execute_step(name, cmd)
        results.append(step_result)
        
        if step_result["status"] != "success":
            overall_status = "failed"
            logging.error(f"[STOP] PIPELINE FAILED at {name} - stopping")
            break

    pipeline_end = datetime.now()
    
    report = {
        "pipeline_execution_id": f"PIPE_{pipeline_start.strftime('%Y%m%d_%H%M%S')}",
        "start_time": pipeline_start.isoformat(),
        "end_time": pipeline_end.isoformat(),
        "total_duration_seconds": round((pipeline_end - pipeline_start).total_seconds(), 2),
        "status": overall_status,
        "steps_executed": results,
        "data_quality_summary": {
            "quality_score": 100 if overall_status == "success" else 0,
            "critical_issues": sum(1 for r in results if r["status"] == "failed"),
            "errors": [r["error"] for r in results if r["error"]],
            "warnings": []
        }
    }
    
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"PIPELINE COMPLETE - Status: {overall_status} ({report['total_duration_seconds']}s)")
    print(f"Pipeline status: {overall_status}")
    print(f"Report written to: {REPORT_PATH}")

if __name__ == "__main__":
    main()
