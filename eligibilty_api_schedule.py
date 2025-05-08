from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import time
import schedule
import warnings
import logging
import threading
from src.utils import *
from functools import partial
import traceback
import socket
import sys

# Import our alert system
from alert_system import send_alert, error_handler, test_alert_system

# Suppress the specific UserWarning
warnings.filterwarnings("ignore", category=UserWarning, message="Could not infer format")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log", encoding="utf-8"),  # Specify encoding
        logging.StreamHandler()  # Console output
    ]
)

logger = logging.getLogger(__name__)

def safe_execute(func, job_name, *args, **kwargs):
    """
    Execute a function safely, catching and reporting any exceptions.
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        error_info = f"{str(e)}\n\n{traceback.format_exc()}"
        error_handler(job_name, error_info)

def _eligibility_iqama_job(query, source):
    job_name = f"IQAMA_JOB_{source}"
    
    logger.info(f"[{source}] ===== Starting IQAMA job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    
    try:
        today = datetime.today().strftime('%Y-%m-%d')
        time_window = datetime.today() - timedelta(hours=4)
        time_window_str = time_window.strftime('%Y-%m-%d %H:%M')

        logger.info(f"[{source}] Fetching data since {time_window_str}")
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')

        engine = get_conn_engine(source=source)
        df_new = pd.read_sql_query(query, engine)
        df_new = df_new.apply(map_row, axis = 1)
        df_new.to_csv(f"data/in/IN_DATA_{timestamp}.csv")

        if df_new.empty:
            logger.info(f"[{source}] No new data to process")
            return
        
        logger.info(f"[{source}] Fetched {len(df_new)} new records")
        df_new = df_new.drop_duplicates(keep='last').reset_index(drop=True)
        
        result_df = Iqama_table(df_new)
        result_df['Insertion_Date'] = datetime.today().strftime('%Y-%m-%d %H:%M')
        

        if source == "ORACLE_LIVE":
            result_df.to_csv(f"data/OSIS/iqama_{timestamp}.csv")
            update_table(table_name="dbo.Iqama_data", df=result_df)
        elif source == "AHJ_DOT-CARE":
            result_df.to_csv(f"data/DOT-CARE/iqama_{timestamp}.csv")
            update_table(table_name="Iqama_dotcare", df=result_df)
        
        logger.info(f"[{source}] Starting eligibility API requests...")

        df_new["start_date"] = df_new.apply(lambda row: change_date(str(row['start_date'])), axis=1)
        df_new["end_date"] = df_new.apply(lambda row: change_date(str(row['end_date'])), axis=1)
        df_new["date_of_birth"] = df_new.apply(lambda row: change_date(str(row['date_of_birth'])), axis=1)
        

        tqdm.pandas(desc=f"[{source}] API Requests")
        df_new["elgability_response"] = df_new.progress_apply(
            lambda row: send_json_to_api(create_json_payload(row, source=source)), 
            axis=1
        )
        df_new.to_csv(f"data/in/before_api_{timestamp}.csv")

        df_new["class"] = df_new.apply(lambda row: extract_code(row["elgability_response"]), axis=1)
        df_new["outcome"] = df_new.apply(lambda row: extract_outcome(row["elgability_response"]), axis=1)
        df_new["note"] = df_new.apply(lambda row: extract_note(row["elgability_response"]), axis=1)
        df_new['insertion_date'] = datetime.today().strftime('%Y-%m-%d %H:%M')
        df_new = df_new.dropna()

        if source == "ORACLE_LIVE":
            df = df_new[["patient_id", "episode_no", "outcome", "note", "class", "insertion_date"]]
            df.to_csv(f"data/OSIS/eligibilty_{timestamp}.csv")
            update_table(table_name="EligibilityResponses", df=df)
        elif source == "AHJ_DOT-CARE":
            df = df_new[["visit_id", "outcome", "note", "class", "insertion_date"]]
            df.to_csv(f"data/DOT-CARE/eligibilty_{timestamp}.csv")
            update_table(table_name="Eligibility_dotcare", df=df)

        logger.info(f"[{source}] ===== Job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====\n")

    except Exception as e:
        error_info = f"{str(e)}\n\n{traceback.format_exc()}"
        error_handler(job_name, error_info)
        
def run_scheduler():
    """
    Runs the scheduler in a separate thread.
    """
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            error_info = f"{str(e)}\n\n{traceback.format_exc()}"
            error_handler("Scheduler_Thread", error_info)


# Function to check if current time is in blackout period (11 PM to 3 AM)
def is_blackout_period():
    current_hour = datetime.now().hour
    return 24 <= current_hour or current_hour < 2


# Wrapper function to run the job only outside blackout period
def run_job_if_allowed(elgability_query, source):
    job_name = f"SCHEDULED_JOB_{source}"
    if not is_blackout_period():
        logger.info("Running scheduled job.")
        safe_execute(_eligibility_iqama_job, job_name, elgability_query, source)
    else:
        logger.info("Skipping job execution during blackout period (11 PM to 3 AM).")


if __name__ == '__main__':
    # Check if this is a test run
    if "--test-alert" in sys.argv:
        # Only run the alert test
        test_alert_system()
        logger.info("Alert test completed. Exiting.")
        sys.exit(0)
    
    # Test the alert system first
    try:
        test_alert_system()
    except Exception as e:
        logger.error(f"Error during alert test: {str(e)}")
        logger.info("Continuing with scheduler despite alert test failure")
    
    try:
        with open("C:\Data-Science\Deployment\Eligibilty_api_schedule\query\eligibilty_dotcare.sql", "r") as file:
            eligibilty_dotcare = file.read()

        with open(r"C:\Data-Science\Deployment\Eligibilty_api_schedule\query\eligibilty_osis.sql", "r") as file:
            eligibilty_osis = file.read()
    except Exception as e:
        error_message = f"Failed to load SQL queries: {str(e)}"
        logger.critical(error_message)
        send_alert("CRITICAL - Failed to start scheduler", error_message)
        sys.exit(1)

    # Check if current time is in blackout period
    if is_blackout_period():
        logger.info("Current time is in blackout period (12 PM to 2 AM). Waiting until 2 AM to run the first job.")
        
        # Calculate time until 3 AM
        now = datetime.now()
        if now.hour >= 2:  # If it's past 3 AM today, wait until tomorrow
            target_time = (now + timedelta(days=1)).replace(hour=2, minute=0, second=0, microsecond=0)
        else:  # If it's before 3 AM, wait until 3 AM today
            target_time = now.replace(hour=2, minute=0, second=0, microsecond=0)
        
        # Calculate seconds to wait
        seconds_to_wait = (target_time - now).total_seconds()
        logger.info(f"Waiting for {seconds_to_wait/60:.1f} minutes until {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Sleep until 3 AM
        time.sleep(seconds_to_wait)

    # Run the job immediately using safe_execute
    logger.info("Running job for the first time.")
    #safe_execute(_eligibility_iqama_job, "INITIAL_JOB_ORACLE_LIVE", eligibilty_osis, "ORACLE_LIVE")
    safe_execute(_eligibility_iqama_job, "INITIAL_JOB_DOT-CARE", eligibilty_dotcare, "AHJ_DOT-CARE")
    
    logger.info("First job execution completed.")
    
    # Schedule the job to run every 4 hours, but only if outside blackout period
    logger.info("Setting up scheduler to run every 4 hours (except during blackout period).")
    #schedule.every(4).hours.do(partial(run_job_if_allowed, eligibilty_osis, "ORACLE_LIVE"))
    schedule.every(4).hours.do(partial(run_job_if_allowed, eligibilty_dotcare, "AHJ_DOT-CARE"))
        
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
    
    # Keep the main thread alive
    try:
        while scheduler_thread.is_alive():
            scheduler_thread.join(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        # Send notification about scheduler shutdown
        send_alert(
            "Scheduler Stopped", 
            f"The scheduler was manually stopped on {socket.gethostname()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}."
        )