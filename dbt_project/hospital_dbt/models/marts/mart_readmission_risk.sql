select
    patient_id,
    age,
    gender,
    diagnosis,
    department,
    state,
    insurance_type,
    admission_date,
    discharge_date,
    length_of_stay,
    prior_30day_visits,
    discharge_disposition,
    readmitted_30days,
    age_risk_group,
    los_risk_category,
    high_prior_visit_flag,
    diagnosis_risk_score,
    risk_score,
    case
        when risk_score >= 8 then 'CRITICAL'
        when risk_score >= 6 then 'HIGH'
        when risk_score >= 4 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from {{ ref('int_patient_risk_factors') }}
order by risk_score desc
