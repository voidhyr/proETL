import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENWEATHER_BASE_URL = os.getenv("OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5/weather")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "weather_warehouse")
DB_USER = os.getenv("DB_USER", "etl_user")
raw_password = os.getenv("DB_PASSWORD", "")
DB_PASSWORD = urllib.parse.quote_plus(raw_password)

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_engine():
    return create_engine(DATABASE_URL)

def init_dq_log_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS data_quality_log (
        log_id BIGSERIAL PRIMARY KEY,
        pipeline_run_id VARCHAR(100),
        rule_failed VARCHAR(100) NOT NULL,
        invalid_value TEXT,
        failure_reason TEXT,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(ddl))

if __name__ == "__main__":
    try:
        eng = get_db_engine()
        with eng.connect() as conn:
            res = conn.execute(text("SELECT 1;")).scalar()
            print(f"[SUCCESS] Database connectivity verified. Result: {res}")
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
