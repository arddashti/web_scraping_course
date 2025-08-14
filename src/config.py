import os
import urllib.parse
from sqlalchemy import create_engine
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی
load_dotenv()

# ------------------------------
# Database
# ------------------------------
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")


params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={DB_SERVER};DATABASE={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS}"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

#---------------------
#Database_Schema
#---------------------
TSETMC_SCHEMA = os.getenv("TSETMC_SCHEMA", "tsetmc_api")



# ------------------------------
# TSETMC API
# ------------------------------
TSETMC_USERNAME = os.getenv("TSETMC_USERNAME")
TSETMC_PASSWORD = os.getenv("TSETMC_PASSWORD")
TSETMC_URL = os.getenv("TSETMC_URL")


