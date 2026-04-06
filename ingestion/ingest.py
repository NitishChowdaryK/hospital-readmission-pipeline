import pandas as pd
import snowflake.connector
import logging
from datetime import datetime

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ingestion/ingest.log"),
        logging.StreamHandler()
    ]
)

# --- Config ---
RAW_FILE = "ingestion/patient_discharges.csv"
SNOWFLAKE_CONFIG = {
    "account": "CELALUY-XV02685",
    "user": "NITISHCHOWDARY22",
    "password": "Nitishch@220022",  # Replace this
    "warehouse": "COMPUTE_WH",
    "database": "HOSPITAL_DB",
    "schema": "READMISSION"
}

# --- Extract ---
def extract():
    logging.info("EXTRACT: Loading patient discharge data...")
    df = pd.read_csv(RAW_FILE)
    logging.info(f"EXTRACT: Loaded {len(df):,} records.")
    return df

# --- Validate & Transform ---
def transform(df):
    logging.info("TRANSFORM: Validating and cleaning data...")
    original = len(df)

    # Flag bad records
    df["data_quality_flag"] = "CLEAN"
    df.loc[df["age"].isnull(), "data_quality_flag"] = "NULL_AGE"
    df.loc[df["diagnosis"].isnull(), "data_quality_flag"] = "NULL_DIAGNOSIS"
    df.loc[df["length_of_stay"] < 0, "data_quality_flag"] = "INVALID_LOS"

    # Separate bad records
    bad = df[df["data_quality_flag"] != "CLEAN"]
    clean = df[df["data_quality_flag"] == "CLEAN"].copy()

    logging.info(f"TRANSFORM: {len(bad):,} bad records flagged and removed.")
    logging.info(f"TRANSFORM: {len(clean):,} clean records ready.")

    # Add metadata
    clean["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    clean["age"] = clean["age"].astype(int)

    return clean, bad

# --- Load to Snowflake ---
def load(df):
    logging.info("LOAD: Connecting to Snowflake...")
    from snowflake.connector.pandas_tools import write_pandas

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW_PATIENT_DISCHARGES (
            patient_id VARCHAR(20),
            age INT,
            gender VARCHAR(5),
            diagnosis VARCHAR(100),
            department VARCHAR(100),
            admission_date DATE,
            discharge_date DATE,
            length_of_stay INT,
            prior_30day_visits INT,
            discharge_disposition VARCHAR(100),
            readmitted_30days INT,
            insurance_type VARCHAR(50),
            state VARCHAR(5),
            data_quality_flag VARCHAR(50),
            ingested_at VARCHAR(30)
        )
    """)

    cur.execute("TRUNCATE TABLE RAW_PATIENT_DISCHARGES")

    # Bulk insert using write_pandas
    df.columns = [c.upper() for c in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn, df, "RAW_PATIENT_DISCHARGES",
        database="HOSPITAL_DB", schema="READMISSION"
    )

    cur.close()
    conn.close()
    logging.info(f"LOAD: {nrows:,} records loaded into Snowflake in {nchunks} chunks.")

# --- Summary ---
def print_summary(clean, bad):
    print("\n" + "="*55)
    print("INGESTION SUMMARY")
    print("="*55)
    print(f"Total Records    : {len(clean) + len(bad):,}")
    print(f"Clean Records    : {len(clean):,}")
    print(f"Flagged Records  : {len(bad):,}")
    print(f"Diagnoses        : {clean['DIAGNOSIS'].nunique()}")
    print(f"Departments      : {clean['DEPARTMENT'].nunique()}")
    print(f"States           : {clean['STATE'].nunique()}")
    print(f"Readmission Rate : {clean['READMITTED_30DAYS'].mean()*100:.1f}%")
    print("="*55)

# --- Main ---
if __name__ == "__main__":
    logging.info("="*55)
    logging.info("INGESTION PIPELINE STARTED")
    raw = extract()
    clean, bad = transform(raw)
    load(clean)
    print_summary(clean, bad)
    logging.info("INGESTION PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("="*55)