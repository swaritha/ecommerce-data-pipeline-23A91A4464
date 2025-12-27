import os
import json
import time
import schedule
import subprocess
import logging
from datetime import datetime
import yaml

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler_activity.log'),
        logging.StreamHandler()
    ]
)

def load_config():
    default_config = {"scheduler": {"daily_time": "02:00"}, "retention": {"days": 7}}
    try:
        import yaml
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        default_config.update(config)
    except:
        pass
    return default_config

def run_pipeline():
    lock_file = "data/processed/scheduler.lock"
    
    if os.path.exists(lock_file):
        logging.warning("[SKIP] Pipeline already running - lockfile exists")
        return False
    
    try:
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        
        logging.info("[RUN] Starting scheduled pipeline execution")
        cmd = ["python", "scripts/pipeline/orchestrator.py"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            logging.info("[SUCCESS] Pipeline completed successfully")
            cleanup_old_data()
            return True
        else:
            logging.error(f"[FAILED] Pipeline failed: {result.stderr[:200]}...")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error("[TIMEOUT] Pipeline timeout (10min)")
        return False
    except Exception as e:
        logging.error(f"[ERROR] Unexpected scheduler error: {str(e)}")
        return False
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

def cleanup_old_data():
    config = load_config()
    retention_days = config.get("retention", {}).get("days", 7)
    cutoff_time = time.time() - (retention_days * 24 * 3600)
    total_removed = 0
    
    target_dirs = ["data/raw", "data/staging"]
    preserve_keywords = ["summary", "report", "metadata", "analytics", "execution"]
    
    for dir_path in target_dirs:
        if not os.path.exists(dir_path):
            continue
        removed = 0
        for filename in os.listdir(dir_path):
            filepath = os.path.join(dir_path, filename)
            if os.path.isfile(filepath):
                file_age = os.path.getmtime(filepath)
                if (file_age < cutoff_time and 
                    not any(kw in filename.lower() for kw in preserve_keywords)):
                    try:
                        os.remove(filepath)
                        removed += 1
                    except:
                        pass
        logging.info(f"[CLEANUP] {dir_path}: {removed} files")
        total_removed += removed
    
    logging.info(f"[CLEANUP] Total files removed: {total_removed}")

def main():
    os.makedirs("data/processed", exist_ok=True)
    config = load_config()
    
    logging.info("[START] Scheduler service started")
    logging.info(f"[CONFIG] Daily run: {config['scheduler']['daily_time']}")
    logging.info(f"[CONFIG] Retention: {config['retention']['days']} days")
    
    schedule.every().day.at(config["scheduler"]["daily_time"]).do(run_pipeline)
    schedule.every().day.at("03:00").do(cleanup_old_data)
    
    logging.info("[READY] Scheduler running - press Ctrl+C to stop")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("[STOP] Scheduler stopped by user")
            break

if __name__ == "__main__":
    main()
