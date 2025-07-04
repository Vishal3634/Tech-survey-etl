import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import logging
from emailer import send_failure_email

logging.basicConfig (
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)

def run_etl():
    try:
        start_time = datetime.now()
        total_records = 0
        success_count = 0
        failure_count = 0

        logging.info("🚀 ETL job started.")

        # Load environment variables
        load_dotenv()

        JOTFORM_API_KEY = os.getenv("JOTFORM_API_KEY")
        FORM_ID = os.getenv("FORM_ID")
        DB_HOST = os.getenv("PG_HOST")
        DB_PORT = os.getenv("PG_PORT")
        DB_USER = os.getenv("PG_USER")
        DB_PASSWORD = os.getenv("PG_PASSWORD")
        DB_NAME = os.getenv("PG_DATABASE")

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cursor = conn.cursor()

        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tech_survey_responses (
            submission_id TEXT PRIMARY KEY,
            location TEXT,
            age_group TEXT,
            gender TEXT,
            occupation TEXT,
            devices_used TEXT,
            screen_time TEXT,
            tech_purpose TEXT,
            satisfaction INTEGER,
            most_used_app TEXT,
            submission_date TIMESTAMP
        );
        """)
        conn.commit()

        # Get data from JotForm
        url = f"https://api.jotform.com/form/{FORM_ID}/submissions?apiKey={JOTFORM_API_KEY}"
        response = requests.get(url)
        data = response.json()

        for submission in data.get("content", []):
            total_records+=1
            try:
                submission_id = submission.get("id")
                submission_date_str = submission.get("created_at", "")
                submission_date = datetime.strptime(submission_date_str, "%Y-%m-%d %H:%M:%S") if submission_date_str else None
                answers = submission.get("answers", {})

                # Correct QIDs based on your form structure
                location       = answers.get("4", {}).get("answer", "")
                age_group      = answers.get("5", {}).get("answer", "")
                gender         = answers.get("6", {}).get("answer", "")
                occupation     = answers.get("7", {}).get("answer", "")
                devices_used   = answers.get("8", {}).get("answer", [])
                screen_time    = answers.get("9", {}).get("answer", "")
                tech_purpose   = answers.get("10", {}).get("answer", "")
                satisfaction   = answers.get("11", {}).get("answer", "")
                most_used_app  = answers.get("12", {}).get("answer", "")

                # Handle satisfaction (convert to int)
                try:
                    if isinstance(satisfaction, list):
                        satisfaction= int(satisfaction[0])
                    else:
                        satisfaction= int(satisfaction)
                except:
                    satisfaction = None

                # Devices list -> comma-separated string
                if isinstance(devices_used, list):
                    devices_used_str = ", ".join(devices_used)
                else:
                    devices_used_str = str(devices_used)

                # Insert into PostgreSQL
                insert_query = """
                INSERT INTO tech_survey_responses (
                    submission_id, location, age_group, gender, occupation,
                    devices_used, screen_time, tech_purpose, satisfaction,
                    most_used_app, submission_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (submission_id) DO NOTHING;
                """
                cursor.execute(insert_query, (
                    submission_id, location, age_group, gender, occupation,
                    devices_used_str, screen_time, tech_purpose,
                    satisfaction, most_used_app, submission_date
                ))

                logging.info(f"✅ Inserted: {submission_id}")
                success_count+=1

            except Exception as e:
                logging.error(f"❌ Error inserting submission: {submission.get('id')} - {e}")
                failure_count+=1

            logging.info("✅ ETL job finished.")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ All done.") 

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logging.exception("❌ ETL job failed (critical).")
        error_message = (
            f"ETL job failed.\n\nError: {str(e)}\n"
            f"📊 Metrics Before Failure:\n"
            f"• Total Records: {total_records}\n"
            f"• Success: {success_count}\n"
            f"• Failures: {failure_count}\n"
            f"• Duration: {duration:.2f} seconds"
        )
        send_failure_email(error_message)
        return 


    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logging.info("✅ ETL job finished.")
    logging.info(f"📊 Total Records Processed: {total_records}")
    logging.info(f"✅ Successful Inserts: {success_count}")
    logging.info(f"❌ Failed Inserts: {failure_count}")
    logging.info(f"⏱️ Total Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    run_etl()       