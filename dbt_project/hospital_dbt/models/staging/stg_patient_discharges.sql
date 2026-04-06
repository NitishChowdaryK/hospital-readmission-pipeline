select
    patient_id,
    age,
    gender,
    diagnosis,
    department,
    admission_date::date     as admission_date,
    discharge_date::date     as discharge_date,
    length_of_stay,
    prior_30day_visits,
    discharge_disposition,
    readmitted_30days,
    insurance_type,
    state,
    ingested_at
from HOSPITAL_DB.READMISSION.RAW_PATIENT_DISCHARGES
where patient_id is not null
  and age is not null
  and diagnosis is not null
  and length_of_stay > 0