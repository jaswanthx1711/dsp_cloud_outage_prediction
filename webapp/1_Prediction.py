import streamlit as st
import requests
import pandas as pd
import os
from datetime import datetime

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Prediction", layout="wide")

col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.title("Make Predictions")
with col2:
    if st.button("Past Predictions", use_container_width=True):
        st.switch_page("pages/2_Past_Predictions.py")

mode = st.radio("Prediction Mode", ["Single Prediction", "Batch Prediction (CSV Upload)"], horizontal=True)

CLOUD_PROVIDERS = ["AWS", "GCP", "Azure", "IBM", "Oracle", "Alibaba"]
SERVICES = ["Compute", "Storage", "Database", "AI/ML", "Networking", "Identity", "Analytics", "Security"]
SEVERITIES = ["Low", "Medium", "High", "Critical"]
BACKUP_OPTIONS = ["Yes", "No"]


def build_feature_form(key_suffix=""):
    col1, col2 = st.columns(2)
    with col1:
        cloud_provider = st.selectbox("Cloud Provider", CLOUD_PROVIDERS, key=f"cp_{key_suffix}")
        service = st.selectbox("Service Affected", SERVICES, key=f"svc_{key_suffix}")
        severity = st.selectbox("Incident Severity", SEVERITIES, key=f"sev_{key_suffix}")
    with col2:
        customers = st.number_input("Customers Affected", min_value=0, value=1000, key=f"cust_{key_suffix}")
        tickets = st.number_input("Ticket Count", min_value=0, value=30, key=f"tkt_{key_suffix}")
        backup = st.selectbox("Backup System Triggered", BACKUP_OPTIONS, key=f"bkp_{key_suffix}")

    return {
        "cloud_provider": cloud_provider,
        "service": service,
        "severity": severity,
        "number_of_customers_affected": customers,
        "ticket_count": tickets,
        "backup_system_triggered": backup,
    }


if mode == "Single Prediction":
    st.subheader("Fill in incident attributes")
    with st.form("single_form"):
        features = build_feature_form("single")
        submitted = st.form_submit_button("Predict Outage Duration")

    if submitted:
        payload = {"features": [features], "source": "webapp"}
        try:
            resp = requests.post(f"{API_URL}/predict", json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()["predictions"][0]
            hours = result["predicted_hours"]
            is_anomaly = result["is_anomaly"]
            start_time = result.get("start_time")
            predicted_end = result.get("predicted_end_time")

            st.success(f"Predicted Outage Duration: {hours:.2f} Hours")
            if is_anomaly:
                st.error("Anomaly Detected - Outage exceeds 5 hours!")
            else:
                st.info("Normal outage duration (under 5 hours)")

            st.subheader("Prediction Details")
            display = {**features, "Predicted Hours": hours, "Anomaly": "Yes" if is_anomaly else "No", "Model": result["model_version"]}
            if start_time:
                display["Start Time"] = start_time.replace("T", " ").replace("Z", "")
            if predicted_end:
                display["Predicted End Time"] = predicted_end.replace("T", " ").replace("Z", "")
            st.dataframe(pd.DataFrame([display]))

        except requests.exceptions.ConnectionError:
            st.error("Cannot connect to API. Is it running?")
        except Exception as e:
            st.error(f"Error: {e}")

else:
    st.subheader("Upload inference CSV (no labels needed)")
    uploaded = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded:
        df = pd.read_csv(uploaded)
        st.write("**Preview (first 5 rows):**")
        st.dataframe(df.head())

        if st.button("Predict for All Rows"):
            required = ["cloud_provider", "service", "severity",
                        "number_of_customers_affected", "ticket_count", "backup_system_triggered"]
            missing = [c for c in required if c not in df.columns]
            if missing:
                st.error(f"Missing columns in CSV: {missing}")
            else:
                features_list = df[required].to_dict(orient="records")
                payload = {"features": features_list, "source": "webapp"}
                try:
                    resp = requests.post(f"{API_URL}/predict", json=payload, timeout=30)
                    resp.raise_for_status()
                    preds = resp.json()["predictions"]

                    results_df = df[required].copy()
                    results_df["Predicted Hours"] = [round(p["predicted_hours"], 2) for p in preds]
                    results_df["Anomaly"] = ["Yes" if p["is_anomaly"] else "No" for p in preds]
                    results_df["Model Version"] = [p["model_version"] for p in preds]
                    if "start_time" in preds[0]:
                        results_df["Start Time"] = [p.get("start_time", "").replace("T", " ").replace("Z", "") for p in preds]
                    if "predicted_end_time" in preds[0]:
                        results_df["Predicted End Time"] = [p.get("predicted_end_time", "").replace("T", " ").replace("Z", "") for p in preds]

                    st.success(f"{len(preds)} predictions made!")
                    st.dataframe(results_df)

                except requests.exceptions.ConnectionError:
                    st.error("Cannot connect to API. Is it running?")
                except Exception as e:
                    st.error(f"Error: {e}")

st.divider()
st.subheader("View Historical Predictions")
if st.button("Go to Past Predictions", use_container_width=True, key="nav_past_predictions"):
    st.switch_page("pages/2_Past_Predictions.py")
