import snowflake.connector
import pandas as pd
from datetime import datetime

# --- Config ---
SNOWFLAKE_CONFIG = {
    "account": "CELALUY-XV02685",
    "user": "NITISHCHOWDARY22",
    "password": "Nitishch@220022",  # Replace this
    "warehouse": "COMPUTE_WH",
    "database": "HOSPITAL_DB",
    "schema": "READMISSION"
}

OUTPUT_FILE = "reports/readmission_risk_report.html"

# --- Fetch Data ---
def fetch(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def get_data():
    summary = fetch("""
        SELECT
            COUNT(*) as total_patients,
            SUM(READMITTED_30DAYS) as total_readmissions,
            ROUND(AVG(READMITTED_30DAYS)*100, 1) as readmission_rate,
            ROUND(AVG(RISK_SCORE), 2) as avg_risk_score,
            SUM(CASE WHEN RISK_LEVEL = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN RISK_LEVEL = 'HIGH' THEN 1 ELSE 0 END) as high_count,
            SUM(CASE WHEN RISK_LEVEL = 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,
            SUM(CASE WHEN RISK_LEVEL = 'LOW' THEN 1 ELSE 0 END) as low_count
        FROM MART_READMISSION_RISK
    """)

    high_risk = fetch("""
        SELECT PATIENT_ID, AGE, GENDER, DIAGNOSIS, DEPARTMENT,
               STATE, INSURANCE_TYPE, LENGTH_OF_STAY,
               PRIOR_30DAY_VISITS, RISK_SCORE, RISK_LEVEL
        FROM MART_READMISSION_RISK
        WHERE RISK_LEVEL IN ('CRITICAL', 'HIGH')
        ORDER BY RISK_SCORE DESC
        LIMIT 20
    """)

    diagnosis = fetch("""
        SELECT DIAGNOSIS, TOTAL_PATIENTS, TOTAL_READMISSIONS,
               READMISSION_RATE_PCT, AVG_RISK_SCORE, HIGH_RISK_COUNT
        FROM MART_DIAGNOSIS_SUMMARY
        ORDER BY READMISSION_RATE_PCT DESC
    """)

    department = fetch("""
        SELECT DEPARTMENT, TOTAL_PATIENTS, TOTAL_READMISSIONS,
               READMISSION_RATE_PCT, AVG_RISK_SCORE,
               CRITICAL_RISK_COUNT, HIGH_RISK_COUNT
        FROM MART_DEPARTMENT_SUMMARY
        ORDER BY AVG_RISK_SCORE DESC
    """)

    return summary, high_risk, diagnosis, department

# --- Generate HTML ---
def generate_html(summary, high_risk, diagnosis, department):
    s = summary.iloc[0]
    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    def risk_badge(level):
        colors = {"CRITICAL": "#dc2626", "HIGH": "#ea580c", "MEDIUM": "#d97706", "LOW": "#16a34a"}
        return f'<span style="background:{colors.get(level,"#888")};color:#fff;padding:3px 10px;border-radius:12px;font-size:0.75rem;font-weight:700">{level}</span>'

    def diag_rows():
        rows = ""
        for _, r in diagnosis.iterrows():
            rows += f"""<tr>
                <td><strong>{r['DIAGNOSIS']}</strong></td>
                <td>{int(r['TOTAL_PATIENTS']):,}</td>
                <td>{int(r['TOTAL_READMISSIONS']):,}</td>
                <td><strong style="color:#dc2626">{r['READMISSION_RATE_PCT']}%</strong></td>
                <td>{r['AVG_RISK_SCORE']}</td>
                <td>{int(r['HIGH_RISK_COUNT']):,}</td>
            </tr>"""
        return rows

    def dept_rows():
        rows = ""
        for _, r in department.iterrows():
            rows += f"""<tr>
                <td><strong>{r['DEPARTMENT']}</strong></td>
                <td>{int(r['TOTAL_PATIENTS']):,}</td>
                <td>{int(r['TOTAL_READMISSIONS']):,}</td>
                <td><strong style="color:#dc2626">{r['READMISSION_RATE_PCT']}%</strong></td>
                <td>{r['AVG_RISK_SCORE']}</td>
                <td style="color:#dc2626"><strong>{int(r['CRITICAL_RISK_COUNT']):,}</strong></td>
                <td style="color:#ea580c"><strong>{int(r['HIGH_RISK_COUNT']):,}</strong></td>
            </tr>"""
        return rows

    def patient_rows():
        rows = ""
        for _, r in high_risk.iterrows():
            rows += f"""<tr>
                <td>{r['PATIENT_ID']}</td>
                <td>{int(r['AGE'])}</td>
                <td>{r['GENDER']}</td>
                <td>{r['DIAGNOSIS']}</td>
                <td>{r['DEPARTMENT']}</td>
                <td>{r['INSURANCE_TYPE']}</td>
                <td>{int(r['LENGTH_OF_STAY'])} days</td>
                <td>{int(r['PRIOR_30DAY_VISITS'])}</td>
                <td><strong>{int(r['RISK_SCORE'])}/10</strong></td>
                <td>{risk_badge(r['RISK_LEVEL'])}</td>
            </tr>"""
        return rows

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Hospital Readmission Risk Report</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; background: #f8fafc; margin: 0; padding: 0; color: #1e293b; }}
  .header {{ background: linear-gradient(135deg, #1e3a5f, #2563eb); color: white; padding: 36px 48px; }}
  .header h1 {{ font-size: 1.8rem; font-weight: 800; margin-bottom: 6px; }}
  .header p {{ opacity: 0.8; font-size: 0.9rem; }}
  .content {{ padding: 36px 48px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 36px; }}
  .kpi {{ background: white; border-radius: 10px; padding: 20px 24px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-top: 4px solid #2563eb; }}
  .kpi h3 {{ font-size: 2rem; font-weight: 800; color: #2563eb; margin-bottom: 4px; }}
  .kpi p {{ font-size: 0.8rem; color: #64748b; }}
  .kpi.red {{ border-top-color: #dc2626; }} .kpi.red h3 {{ color: #dc2626; }}
  .kpi.orange {{ border-top-color: #ea580c; }} .kpi.orange h3 {{ color: #ea580c; }}
  .kpi.green {{ border-top-color: #16a34a; }} .kpi.green h3 {{ color: #16a34a; }}
  .section {{ background: white; border-radius: 10px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .section h2 {{ font-size: 1.1rem; font-weight: 700; margin-bottom: 16px; color: #1e293b; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th {{ background: #1e3a5f; color: white; padding: 10px 12px; text-align: left; font-size: 0.78rem; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #f1f5f9; }}
  tr:hover {{ background: #f8fafc; }}
  .footer {{ text-align: center; padding: 20px; font-size: 0.8rem; color: #94a3b8; }}
</style>
</head>
<body>
<div class="header">
  <h1>🏥 Hospital Readmission Risk Report</h1>
  <p>Generated: {generated} &nbsp;|&nbsp; Data Source: Snowflake HOSPITAL_DB &nbsp;|&nbsp; Pipeline: Python → Snowflake → dbt</p>
</div>
<div class="content">

  <!-- KPIs -->
  <div class="kpi-grid">
    <div class="kpi"><h3>{int(s['TOTAL_PATIENTS']):,}</h3><p>Total Patients</p></div>
    <div class="kpi"><h3>{s['READMISSION_RATE']}%</h3><p>30-Day Readmission Rate</p></div>
    <div class="kpi red"><h3>{int(s['CRITICAL_COUNT']):,}</h3><p>Critical Risk Patients</p></div>
    <div class="kpi orange"><h3>{int(s['HIGH_COUNT']):,}</h3><p>High Risk Patients</p></div>
  </div>

  <!-- High Risk Patients -->
  <div class="section">
    <h2>⚠️ High Risk Patients (Top 20)</h2>
    <table>
      <tr><th>Patient ID</th><th>Age</th><th>Gender</th><th>Diagnosis</th><th>Department</th>
          <th>Insurance</th><th>Length of Stay</th><th>Prior Visits</th><th>Risk Score</th><th>Risk Level</th></tr>
      {patient_rows()}
    </table>
  </div>

  <!-- Diagnosis Summary -->
  <div class="section">
    <h2>📋 Readmission Rate by Diagnosis</h2>
    <table>
      <tr><th>Diagnosis</th><th>Total Patients</th><th>Readmissions</th>
          <th>Readmission Rate</th><th>Avg Risk Score</th><th>High Risk Count</th></tr>
      {diag_rows()}
    </table>
  </div>

  <!-- Department Summary -->
  <div class="section">
    <h2>🏨 Risk Breakdown by Department</h2>
    <table>
      <tr><th>Department</th><th>Total Patients</th><th>Readmissions</th>
          <th>Readmission Rate</th><th>Avg Risk Score</th><th>Critical</th><th>High Risk</th></tr>
      {dept_rows()}
    </table>
  </div>

</div>
<div class="footer">Hospital Readmission Risk Pipeline · Built by Nitish Kolupoti · Data Engineering Portfolio</div>
</body>
</html>"""

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report saved to {OUTPUT_FILE}")

# --- Main ---
if __name__ == "__main__":
    print("Fetching data from Snowflake...")
    summary, high_risk, diagnosis, department = get_data()
    print("Generating HTML report...")
    generate_html(summary, high_risk, diagnosis, department)
    print("Done! Open reports/readmission_risk_report.html in your browser.")