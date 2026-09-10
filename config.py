import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy import text

# 1. Load environment variables from .env
load_dotenv()

# 2. OpenWeatherMap Configuration
OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL: str = os.getenv(
    "OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5/weather"
)

# 3. PostgreSQL Warehouse Configuration
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")
DB_NAME: str = os.getenv("DB_NAME", "weather_warehouse")
DB_USER: str = os.getenv("DB_USER", "etl_user")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

# 4. Construct SQLAlchemy Database URL
# Format: postgresql+psycopg2://user:password@host:port/dbname
if DB_PASSWORD:
    DATABASE_URL: str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
else:
    DATABASE_URL: str = (
        f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy Engine instance for the data warehouse."""
    return create_engine(DATABASE_URL, echo=False)


def test_db_connection() -> bool:
    """Tests the database connection by executing a simple SELECT 1 query."""
    print("Testing connection to PostgreSQL warehouse...")
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            db_version = result.scalar()
            print("[SUCCESS] Connected to database:", DB_NAME)
            print("[INFO] PostgreSQL Version:", db_version)
            return True
    except Exception as e:
        print("[ERROR] Database connection failed:", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        return False


def init_dq_log_table() -> None:
    """Creates the data_quality_log table in PostgreSQL if it does not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS data_quality_log (
        log_id BIGSERIAL PRIMARY KEY,
        pipeline_run_id VARCHAR(100),
        city_name VARCHAR(100),
        rule_failed VARCHAR(100) NOT NULL,
        invalid_value TEXT,
        failure_reason TEXT,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("[SUCCESS] Verified data_quality_log table in PostgreSQL.")

if __name__ == "__main__":
    success = test_db_connection()
    sys.exit(0 if success else 1)
