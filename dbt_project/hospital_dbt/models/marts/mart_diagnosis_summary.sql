select
    diagnosis,
    count(*)                                        as total_patients,
    sum(readmitted_30days)                          as total_readmissions,
    round(avg(readmitted_30days) * 100, 2)          as readmission_rate_pct,
    round(avg(risk_score), 2)                       as avg_risk_score,
    round(avg(length_of_stay), 2)                   as avg_length_of_stay,
    round(avg(age), 2)                              as avg_patient_age,
    sum(case when risk_score >= 6 then 1 else 0 end) as high_risk_count
from {{ ref('int_patient_risk_factors') }}
group by diagnosis
order by readmission_rate_pct desc