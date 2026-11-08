from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
from datetime import datetime, timedelta

from tmdb_transform import CREATE_TABLE_SQL, TARGET_TABLE, load_records, transform_movies

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the ETL DAG
tmdb_dag = DAG(
    'etl_tmdb_movies',
    default_args=default_args,
    description='ETL process for fetching TMDb movies released strictly in the last 7 days',
    schedule_interval='0 0 * * 0',  # Run every Sunday at midnight
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Fetch the last processed date from Airflow variable or Snowflake
@task
def get_last_processed_date():
    try:
        # Attempt to get the last processed date from Airflow Variable or metadata table
        last_processed_date = Variable.get("last_processed_date", default_var=None)  # Default is None if not set
        print(f"Last processed date from Variable: {last_processed_date}")
    except Exception as e:
        last_processed_date = None
        print(f"Could not fetch last processed date: {e}")
    return last_processed_date

# Task 2: Fetch movies released strictly in the past 7 days
@task
def extract_tmdb_movies_last_seven_days():
    api_key = Variable.get("tmdb_api_key")  # Fetch API key from Airflow Variables
    # Fetch movies released in the last 7 days (strictly)
    date_seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    today_date = datetime.now().strftime('%Y-%m-%d')

    url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&region=US&primary_release_date.gte={date_seven_days_ago}&primary_release_date.lte={today_date}"

    all_movies = []
    page = 1
    while True:
        response = requests.get(f"{url}&page={page}")
        
        if response.status_code == 200:
            data = response.json()
            movies = data.get('results', [])
            all_movies.extend(movies)
            
            # If there are no more pages, break the loop
            if page >= data['total_pages']:
                break
            page += 1
        else:
            raise Exception(f"Failed to fetch TMDb data. HTTP Status: {response.status_code}")
    
    print(f"Extracted {len(all_movies)} movies released in the last 7 days.")
    return all_movies

@task
def transform_tmdb_data(data: list):
    """
    Keep movies released in the last 7 days, map genre IDs to names and clean the
    popularity and vote metrics. Zeros are kept; missing or invalid values become
    None and are stored as NULL. See tmdb_transform.py and airflow/tests/.
    """
    if not data:
        print("No data provided for transformation.")
        return []

    transformed = transform_movies(data)
    print(f"Transformed {len(transformed)} movies released in the last week.")
    return transformed


# Task 4: Load TMDb data into Snowflake incrementally
@task
def load_tmdb_data_to_snowflake(records: list):
    if not records:
        print("No records to load into Snowflake.")
        return

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        print("Starting transaction...")
        cur.execute("BEGIN;")
        cur.execute(CREATE_TABLE_SQL.format(table=TARGET_TABLE))

        # MERGE with bound parameters (incremental upsert; None values load as NULL)
        loaded = load_records(cur, records, TARGET_TABLE)

        conn.commit()
        print(f"Successfully loaded {loaded} records into Snowflake table: {TARGET_TABLE}.")

        # Update the last processed date to the latest movie's release date
        latest_release_date = max([datetime.strptime(r['release_date'], "%Y-%m-%d") for r in records])
        Variable.set("last_processed_date", latest_release_date.strftime("%Y-%m-%d"))
        print(f"Last processed date updated to: {latest_release_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        conn.rollback()
        print(f"Error during loading data: {e}")
        raise  # fail the task so a bad load is visible in Airflow
    finally:
        cur.close()
        conn.close()


# Define task dependencies
with tmdb_dag:
    new_tmdb_movies = extract_tmdb_movies_last_seven_days()
    transformed_movies = transform_tmdb_data(new_tmdb_movies)
    load_tmdb_data_to_snowflake(transformed_movies)
