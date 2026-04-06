import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# --- Config ---
NUM_RECORDS = 5000
OUTPUT_FILE = "ingestion/patient_discharges.csv"

DIAGNOSES = [
    "Heart Failure", "Pneumonia", "COPD", "Hip/Knee Replacement",
    "Cardiac Arrhythmia", "Diabetes", "Stroke", "Sepsis",
    "Kidney Disease", "Hypertension"
]

DEPARTMENTS = [
    "Cardiology", "Pulmonology", "Orthopedics",
    "General Medicine", "Neurology", "Nephrology", "ICU"
]

DISCHARGE_DISPOSITIONS = [
    "Home", "Home with Care", "Skilled Nursing Facility",
    "Rehabilitation", "Long-Term Care"
]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_records(n):
    records = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)

    for i in range(n):
        age = random.randint(18, 95)
        diagnosis = random.choice(DIAGNOSES)
        los = random.randint(1, 30)  # length of stay in days
        prior_visits = random.randint(0, 5)
        discharge_date = random_date(start_date, end_date)
        admission_date = discharge_date - timedelta(days=los)

        # Introduce some dirty data intentionally
        if random.random() < 0.03:
            age = None  # 3% null ages
        if random.random() < 0.02:
            diagnosis = None  # 2% null diagnoses
        if random.random() < 0.01:
            los = -1  # 1% invalid LOS

        records.append({
            "patient_id": f"P{str(i+1).zfill(6)}",
            "age": age,
            "gender": random.choice(["M", "F"]),
            "diagnosis": diagnosis,
            "department": random.choice(DEPARTMENTS),
            "admission_date": admission_date.strftime("%Y-%m-%d"),
            "discharge_date": discharge_date.strftime("%Y-%m-%d"),
            "length_of_stay": los,
            "prior_30day_visits": prior_visits,
            "discharge_disposition": random.choice(DISCHARGE_DISPOSITIONS),
            "readmitted_30days": random.choices([1, 0], weights=[0.2, 0.8])[0],
            "insurance_type": random.choice(["Medicare", "Medicaid", "Private", "Uninsured"]),
            "state": fake.state_abbr()
        })
    return records

if __name__ == "__main__":
    print(f"Generating {NUM_RECORDS} patient discharge records...")
    records = generate_records(NUM_RECORDS)
    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Dataset saved to {OUTPUT_FILE}")
    print(f"Shape: {df.shape}")
    print(f"\nSample:")
    print(df.head(3).to_string())
    print(f"\nNull counts:")
    print(df.isnull().sum())