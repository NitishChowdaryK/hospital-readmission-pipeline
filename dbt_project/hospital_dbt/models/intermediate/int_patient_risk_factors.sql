select
    patient_id,
    age,
    gender,
    diagnosis,
    department,
    admission_date,
    discharge_date,
    length_of_stay,
    prior_30day_visits,
    discharge_disposition,
    readmitted_30days,
    insurance_type,
    state,

    -- Age group risk
    case
        when age >= 80 then 'Very High (80+)'
        when age >= 65 then 'High (65-79)'
        when age >= 45 then 'Medium (45-64)'
        else 'Low (18-44)'
    end as age_risk_group,

    -- Length of stay risk
    case
        when length_of_stay >= 14 then 'High (14+ days)'
        when length_of_stay >= 7  then 'Medium (7-13 days)'
        when length_of_stay <= 2  then 'High (1-2 days)'
        else 'Low (3-6 days)'
    end as los_risk_category,

    -- Prior visit risk flag
    case
        when prior_30day_visits >= 2 then 1
        else 0
    end as high_prior_visit_flag,

    -- Diagnosis risk score
    case
        when diagnosis in ('Heart Failure', 'COPD', 'Pneumonia', 'Sepsis') then 3
        when diagnosis in ('Stroke', 'Kidney Disease', 'Cardiac Arrhythmia') then 2
        else 1
    end as diagnosis_risk_score,

    -- Composite risk score (1-10)
    least(10, greatest(1,
        case when age >= 80 then 3 when age >= 65 then 2 else 1 end
        + case when length_of_stay >= 14 or length_of_stay <= 2 then 2 else 1 end
        + case when prior_30day_visits >= 2 then 2 else 0 end
        + case when diagnosis in ('Heart Failure','COPD','Pneumonia','Sepsis') then 3
               when diagnosis in ('Stroke','Kidney Disease','Cardiac Arrhythmia') then 2
               else 1 end
    )) as risk_score

from {{ ref('stg_patient_discharges') }}