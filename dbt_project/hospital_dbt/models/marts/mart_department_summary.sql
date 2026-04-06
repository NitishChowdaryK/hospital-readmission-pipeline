select
    department,
    count(*)                                        as total_patients,
    sum(readmitted_30days)                          as total_readmissions,
    round(avg(readmitted_30days) * 100, 2)          as readmission_rate_pct,
    round(avg(risk_score), 2)                       as avg_risk_score,
    round(avg(length_of_stay), 2)                   as avg_length_of_stay,
    sum(case when risk_score >= 8 then 1 else 0 end) as critical_risk_count,
    sum(case when risk_score >= 6 then 1 else 0 end) as high_risk_count,
    sum(case when risk_score >= 4 then 1 else 0 end) as medium_risk_count
from {{ ref('int_patient_risk_factors') }}
group by department
order by avg_risk_score desc