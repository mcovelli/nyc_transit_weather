import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def get_engine() -> Engine:
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DATABASE", "nyc_transit_weather")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
