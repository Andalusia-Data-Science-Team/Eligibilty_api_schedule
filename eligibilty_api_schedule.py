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

# Suppress the specific UserWarning
warnings.filterwarnings("ignore", category=UserWarning, message="Could not infer format")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

logger = logging.getLogger(__name__)

def _eligibility_iqama_job(query, source):
    """
    Main job function to fetch data, process it, and update the database.
    """
    
    logger.info(f"===== Starting IQAMA job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    
    # Get dates
    today = datetime.today().strftime('%Y-%m-%d')
    time_window = datetime.today() - timedelta(hours=4)
    time_window_str = time_window.strftime('%Y-%m-%d %H:%M')
    
    logger.info(f"Fetching data since {time_window_str}")

    # Define Data source
    engine = get_conn_engine(source = source)
    
    # Fetch new data
    df_new = pd.read_sql_query(query, engine)
    if df_new.empty:
        logger.info("No new data to process")
        return
    
    logger.info(f"Fetched {len(df_new)} new records")
    df_new = df_new.drop_duplicates(keep='last')
    # Important: Reset index to prevent duplicate indices
    df_new = df_new.reset_index(drop=True)
    
    result_df = Iqama_table(df_new)
    
    # Add back the is_new_record flag
    result_df['Insertion_Date'] = datetime.today().strftime('%Y-%m-%d %H:%M')
        
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    if source == "ORACLE_LIVE":
        result_df.to_csv(f"iqama_data/OSIS/iqama_{timestamp}.csv")
        update_table(table_name="dbo.Iqama_data", df=result_df)
    elif source == "AHJ_DOT-CARE":
        result_df.to_csv(f"iqama_data/DOT-CARE/iqama_{timestamp}.csv")
        update_table(table_name="Iqama_dotcare", df=result_df)
    
    logger.info(f"===== Starting eligability job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    
    df_new["start_date"] = df_new.apply(lambda row: change_date(str(row['start_date'])), axis=1)
    df_new["end_date"] = df_new.apply(lambda row: change_date(str(row['end_date'])), axis=1)
    df_new["date_of_birth"] = df_new.apply(lambda row: change_date(str(row['date_of_birth'])), axis=1) 
    
    logger.info("Sending JSON payloads to API...")
    tqdm.pandas(desc="API Requests")
    df_new["elgability_response"] = df_new.progress_apply(
        lambda row: send_json_to_api(create_json_payload(row)), 
        axis=1
    )
        
    df_new["class"] = df_new.apply(lambda row: extract_code(row["elgability_response"]), axis=1)  # Extract the class from the API response
    df_new["outcome"] = df_new.apply(lambda row: extract_outcome(row["elgability_response"]), axis=1)  # Extract the outcome from the API response
    df_new["note"] = df_new.apply(lambda row: extract_note(row["elgability_response"]), axis=1)  # Extract the messege from the API response
    df_new['insertion_date'] = datetime.today().strftime('%Y-%m-%d %H:%M')
    df_new = df_new.dropna()
    if source == "ORACLE_LIVE":
        df = df_new[["patient_id", "episode_no", "outcome", "note", "class", "insertion_date"]]
        update_table(table_name="EligibilityResponses", df=df)
    elif source == "AHJ_DOT-CARE":
        df = df_new[["visit_id", "outcome", "note", "class", "insertion_date"]]
        update_table(table_name="Eligibility_dotcare", df=df)
        
    

    logger.info(f"===== Job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====\n")

        
def run_scheduler():
    """
    Runs the scheduler in a separate thread.
    """
    while True:
        schedule.run_pending()
        time.sleep(60)


# Function to check if current time is in blackout period (11 PM to 3 AM)
def is_blackout_period():
    current_hour = datetime.now().hour
    return 24 <= current_hour or current_hour < 2


# Wrapper function to run the job only outside blackout period
def run_job_if_allowed(elgability_query, source):
    if not is_blackout_period():
        logger.info("Running scheduled job.")
        _eligibility_iqama_job(elgability_query, source)
    else:
        logger.info("Skipping job execution during blackout period (11 PM to 3 AM).")


if __name__ == '__main__':
    with open("C:\Data-Science\Deployment\claims-test-main\XplainClaims\queries\eligibilty_dotcare.sql", "r") as file:
        eligibilty_dotcare = file.read()

    with open(r"C:\Data-Science\Deployment\claims-test-main\XplainClaims\queries\eligibilty_osis.sql", "r") as file:
        eligibilty_osis = file.read()

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

    # Run the job immediately
    logger.info("Running job for the first time.")
    _eligibility_iqama_job(eligibilty_osis, source = "ORACLE_LIVE")
    _eligibility_iqama_job(eligibilty_dotcare, source = "AHJ_DOT-CARE")
    
    logger.info("First job execution completed.")
    
    # Schedule the job to run every 4 hours, but only if outside blackout period
    logger.info("Setting up scheduler to run every 4 hours (except during blackout period).")
    schedule.every(4).hours.do(partial(run_job_if_allowed, eligibilty_osis, source = "ORACLE_LIVE"))
    schedule.every(4).hours.do(partial(run_job_if_allowed, eligibilty_dotcare, source = "AHJ_DOT-CARE"))
    
    # Also keep the daily schedule at 20:40, which is 08:40 PM (before blackout period)
    # logger.info("Setting up scheduler to run every day at 20:40.")
    # schedule.every().day.at("20:40").do(partial(run_job_if_allowed, db_query, elgability_query_per_day))
        
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
    
    # Keep the main thread alive
    try:
        while scheduler_thread.is_alive():
            scheduler_thread.join(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")