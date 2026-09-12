import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# Create database engine
conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

export_dir = PROJECT_ROOT / "bi_exports"
export_dir.mkdir(parents=True, exist_ok=True)

with engine.connect() as conn:
    # 1. Export analytical weather facts view
    query_weather = text("SELECT * FROM vw_weather_analytics ORDER BY ingested_at DESC;")
    res_weather = conn.execute(query_weather)
    df_weather = pd.DataFrame(res_weather.fetchall(), columns=res_weather.keys())
    
    weather_path = export_dir / "vw_weather_analytics.csv"
    df_weather.to_csv(weather_path, index=False)
    print(f"[OK] Exported {len(df_weather)} rows to {weather_path}")

    # 2. Export data quality audit summary view
    query_dq = text("SELECT * FROM vw_dq_summary;")
    res_dq = conn.execute(query_dq)
    df_dq = pd.DataFrame(res_dq.fetchall(), columns=res_dq.keys())
    
    dq_path = export_dir / "vw_dq_summary.csv"
    df_dq.to_csv(dq_path, index=False)
    print(f"[OK] Exported {len(df_dq)} rows to {dq_path}")
