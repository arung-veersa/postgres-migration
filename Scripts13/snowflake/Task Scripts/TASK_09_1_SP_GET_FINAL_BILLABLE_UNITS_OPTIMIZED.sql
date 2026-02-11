CREATE OR REPLACE PROCEDURE CONFLICTREPORT.PUBLIC.SP_GET_FINAL_BILLABLE_UNITS_OPTIMIZED()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'get_billable_units'
EXTERNAL_ACCESS_INTEGRATIONS = (EAI_HHA_REVENUE_API)
EXECUTE AS OWNER
AS '
from decimal import Decimal
import requests
import snowflake.snowpark as snowpark
from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def clean_json(obj):
    if isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_json(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def get_token():
    token_url = "https://idp.cloud.hhaexchange.com/connect/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": "HHAeXchange.Revenue.Api",
        "client_secret": "HT5N6DP6v5eTGP5uJdtmvs5fSRV1uM",
        "scope": "all:read"
    }
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    token_response = requests.post(token_url, data=token_data, headers=token_headers, verify=False)
    token_response.raise_for_status()
    return token_response.json().get("access_token")

def format_time(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def adjust_shift_endtime_for_time_distance(row):
    """
    Adjusts the shift end time by adding ETA travel minutes for distance-based visits
    Returns the adjusted end time or original end time if no adjustment needed
    """
    if row["DistanceFlag"] == "Y" and row["ETATravleMinutes"] is not None and row["ShVTENTime"] is not None:
        try:
            travel_minutes = int(row["ETATravleMinutes"])
            adjusted_end_time = row["ShVTENTime"] + timedelta(minutes=travel_minutes)
            return adjusted_end_time
        except (ValueError, TypeError):
            return row["ShVTENTime"]
    return row["ShVTENTime"]

def get_billable_units(session: snowpark.Session) -> str:
    update_rows = []
    failed_log_rows = []
    failed_ids = []
    BATCH_SIZE = 5000
    MAX_WORKERS = 500
    CHUNK_SIZE = 10000
    try:
        rows_df = session.sql("""
            SELECT 
                c."AppProviderID", 
                c."AppPayerID", 
                d."Environemnt", 
                c."AppVisitID", 
                c."AppOfficeID", 
                c."AppServiceCodeID", 
                c."AppPatientID", 
                c."ShVTSTTime", 
                c."ShVTENTime", 
                c."CShVTSTTime", 
                c."CShVTENTime",
                c.BILLABLEMINUTESFULLSHIFT,
                c.BILLABLEUNITSFULLSHIFT,
                c.BILLABLEMINUTESOVERLAP,
                c.BILLABLEUNITSOVERLAP,
                c.ID,
                c."CONFLICTID",
                c."DistanceFlag",
                c."ETATravleMinutes",
                cr."Application Contract Id" AS ContractIdForInternal
            FROM CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS c
            LEFT JOIN ANALYTICS.BI.DIMPROVIDER d 
                ON c."ProviderID" = d."Provider Id"
            LEFT JOIN ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR cr
                ON c."VisitID" = cr."Visit Id"  
            WHERE d."Environemnt" IN (''PROD:APP'', ''PROD-APP2:AP2'', ''PROD-AWS:CLO'')
                AND c."PTOFlag" = ''N''
                AND c."InServiceFlag" = ''N''
				AND (
                    "SameSchTimeFlag" = ''Y'' OR 
                    "SameVisitTimeFlag" = ''Y'' OR 
                    "SchAndVisitTimeSameFlag" = ''Y'' OR 
                    "SchOverAnotherSchTimeFlag" = ''Y'' OR 
                    "VisitTimeOverAnotherVisitTimeFlag" = ''Y'' OR 
                    "SchTimeOverVisitTimeFlag" = ''Y'' or "DistanceFlag" = ''Y''
                )
				AND c.BILLABLEMINUTESFULLSHIFT IS NULL
                AND c."AppVisitID" IS NOT NULL
				AND c."AppServiceCodeID" IS NOT NULL
				AND (c."P_PAddressState" = ''NY'' OR c."ConP_PAddressState" = ''NY'')
                AND (c.FAILEDON IS NULL OR c.FAILEDON >= DATEADD(DAY, -2, CURRENT_TIMESTAMP()))	  
        """).collect()
		
        if not rows_df:
            return "No data to process."

        token = get_token()
        token_expiry = datetime.now() + timedelta(minutes=8)

        def safe_int(val):
            try:
                return int(val) if val is not None else None
            except:
                return None

        def log_failure(visit_id, payer_id, contract_id, payload, response, failure_type, error_msg, conflict_id, row_id):
            if row_id is None:
                return
            failed_ids.append(row_id)
            failed_log_rows.append((
                visit_id,
                payer_id,
                contract_id,
                json.dumps(payload).replace("''", "''''"),
                json.dumps(response if response else {}).replace("''", "''''"),
                failure_type,
                error_msg.replace("''", "''''"),
                conflict_id
            ))

        def process_row(row):
            nonlocal token, token_expiry
            
            if datetime.now() >= token_expiry:
                token = get_token()
                token_expiry = datetime.now() + timedelta(minutes=8)

            env = row["Environemnt"]
            if env == "PROD:APP":
                api_url = "https://revenueapiapp.hhaexchange.com/v1/billable-units"
            elif env == "PROD-APP2:AP2":
                api_url = "https://revenueapiapp2.hhaexchange.com/v1/billable-units"
            elif env == "PROD-AWS:CLO":
                api_url = "https://revenueapicloud.hhaexchange.com/v1/billable-units"
            else:
                return None

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            payer_id = safe_int(row["AppPayerID"])
            contract_id = safe_int(row["CONTRACTIDFORINTERNAL"])
            app_visit_id = safe_int(row["AppVisitID"])
            row_id = safe_int(row["ID"])
            conflict_id = safe_int(row["CONFLICTID"])
            adjusted_end_time = adjust_shift_endtime_for_time_distance(row)

            if not app_visit_id:
                log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "AppVisitID is null or invalid", conflict_id, row_id)
                return None

            payer_id_to_send = contract_id if (not payer_id or payer_id == 0) else payer_id
            if not payer_id_to_send:
                log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "Both PayerID and ContractID are null or invalid", conflict_id, row_id)
                return None

            try:
                st = row["ShVTSTTime"]
                et0 = row["ShVTENTime"]
                et = adjusted_end_time
                cst = row["CShVTSTTime"]
                cet = row["CShVTENTime"]

                if not st or not et:
                    log_failure(app_visit_id, contract_id, payer_id, None, None, "MissingData", "Start time or end time is null", conflict_id, row_id)
                    return None

                visit_payloads = [{
                    "visitID": app_visit_id,
                    "scheduleIdentifier": f"{row_id}_full",
                    "scheduleStartTime": format_time(st),
                    "scheduleEndTime": format_time(et0),
                    "visitStartTime": format_time(st),
                    "visitEndTime": format_time(et0),
                    "ApprovedTravelTimeMinutes": 0,
                    "adjMinutes": 0,
                    "BankedMinutes": 0
                }]

                if cst and cet:
                    overlap_start, overlap_end = st, et
                    if cst >= st and cst <= et and cet > et:
                        overlap_start, overlap_end = cst, et
                    elif st >= cst and st <= cet and et > cet:
                        overlap_start, overlap_end = st, cet
                    elif cst >= st and cet <= et:
                        overlap_start, overlap_end = cst, cet
                    elif st >= cst and et <= cet:
                        overlap_start, overlap_end = st, et
                    elif cst < st and cet > et:
                        overlap_start, overlap_end = st, et
                    elif st < cst and et > cet:
                        overlap_start, overlap_end = cst, cet

                    visit_payloads.append({
                        "visitID": app_visit_id,
                        "scheduleIdentifier": f"{row_id}_overlap",
                        "scheduleStartTime": format_time(overlap_start),
                        "scheduleEndTime": format_time(overlap_end),
                        "visitStartTime": format_time(overlap_start),
                        "visitEndTime": format_time(overlap_end),
                        "ApprovedTravelTimeMinutes": 0,
                        "adjMinutes": 0,
                        "BankedMinutes": 0
                    })

                payload = {
                    "vendorID": safe_int(row["AppProviderID"]),
                    "payerID": payer_id_to_send,
                    "officeID": safe_int(row["AppOfficeID"]),
                    "servicecodeID": safe_int(row["AppServiceCodeID"]),
                    "patientID": safe_int(row["AppPatientID"]),
                    "userID": 7,
                    "callerInfo": "Conflict",
                    "visits": visit_payloads
                }

                if not payload["vendorID"] or not payload["officeID"] or not payload["servicecodeID"] or not payload["patientID"]:
                    log_failure(app_visit_id, contract_id, payer_id, payload, None, "MissingData", "Required fields (vendorID, officeID, servicecodeID, patientID) are null or invalid", conflict_id, row_id)
                    return None

                for attempt in range(3):
                    try:
                        api_response = requests.post(api_url, json=clean_json(payload), headers=headers, verify=False, timeout=30)
                        break
                    except requests.exceptions.Timeout:
                        if attempt == 2:
                            log_failure(app_visit_id, contract_id, payer_id, payload, None, "Timeout", "API request timed out after 3 attempts", conflict_id, row_id)
                            return None
                        time.sleep(1)
                    except requests.exceptions.RequestException as e:
                        log_failure(app_visit_id, contract_id, payer_id, payload, None, "RequestException", str(e), conflict_id, row_id)
                        return None

                if api_response.status_code == 200:
                    data = api_response.json()
                    visits = data.get("visits", [])
                    
                    config_errors = []
                    for v in visits:
                        if "Message" in v and v["Message"] and "configuration" in v["Message"].lower().strip():
                            config_errors.append(v["Message"])
                    
                    if config_errors:
                        error_msg = "; ".join(config_errors)
                        log_failure(app_visit_id, contract_id, payer_id, payload, data, "ConfigNotFound", error_msg, conflict_id, row_id)
                        return None

                    full = next((v for v in visits if v.get("scheduleIdentifier", "").endswith("_full")), {})
                    overlap = next((v for v in visits if v.get("scheduleIdentifier", "").endswith("_overlap")), {})
                    return (row_id, full.get("billableMinutes", 0), full.get("billableUnits", 0), overlap.get("billableMinutes", 0), overlap.get("billableUnits", 0))
                else:
                    log_failure(app_visit_id, contract_id, payer_id, payload, api_response.text, "API Error", f"Status: {api_response.status_code}", conflict_id, row_id)
                    return None
            except Exception as e:
                log_failure(app_visit_id, contract_id, payer_id, payload, None, "Exception", str(e), conflict_id, row_id)
                return None

        total_processed = 0
        for i in range(0, len(rows_df), BATCH_SIZE):
            batch = rows_df[i:i + BATCH_SIZE]
            batch_start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_row, row) for row in batch]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        update_rows.append(result)
                        total_processed += 1
            
            batch_time = time.time() - batch_start_time
            print(f"Processed batch {i//BATCH_SIZE + 1}: {len(batch)} records in {batch_time:.2f} seconds")

        if update_rows:
            CHUNK_SIZE = 10000
            for i in range(0, len(update_rows), CHUNK_SIZE):
                chunk = update_rows[i:i + CHUNK_SIZE]
                df = session.create_dataframe(chunk, schema=["ID", "BMF", "BUF", "BMO", "BUO"])
                df.write.save_as_table("CONFLICTREPORT.PUBLIC.TMP_BILLABLE_UPDATES", mode="overwrite")
                session.sql("""
                    MERGE INTO CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS t
                    USING CONFLICTREPORT.PUBLIC.TMP_BILLABLE_UPDATES s
                    ON t."ID" = s.ID
                    WHEN MATCHED THEN UPDATE SET 
                        BILLABLEMINUTESFULLSHIFT = s.BMF,
                        BILLABLEUNITSFULLSHIFT = s.BUF,
                        BILLABLEMINUTESOVERLAP = s.BMO,
                        BILLABLEUNITSOVERLAP = s.BUO,
                        FAILEDON = NULL
                """).collect()

        if failed_log_rows:
            for i in range(0, len(failed_log_rows), CHUNK_SIZE):
                chunk = failed_log_rows[i:i + CHUNK_SIZE]
                df_logs = session.create_dataframe(chunk, schema=[
                    "APPVISITID", "APPPAYERID", "CONTRACT_ID_INTERNAL", 
                    "PAYLOAD", "RESPONSE", "FAILURE_TYPE", "ERROR_MESSAGE",
                    "CONFLICTID"
                ])
                df_logs.write.mode("append").save_as_table("CONFLICTREPORT.PUBLIC.FAILED_API_LOGS")

        if failed_ids:
            for i in range(0, len(failed_ids), CHUNK_SIZE):
                chunk_ids = failed_ids[i:i + CHUNK_SIZE]
                id_list = ",".join(str(i) for i in chunk_ids)
                session.sql(f"""
                    UPDATE CONFLICTREPORT.PUBLIC.CONFLICTVISITMAPS
                    SET FAILEDON = CURRENT_TIMESTAMP()
                    WHERE "ID" IN ({id_list}) AND FAILEDON IS NULL
                """).collect()

        return f"Processing completed. Total processed: {total_processed}, Failed: {len(failed_ids)}"

    except Exception as e:
        return f"General exception: {str(e)}"
';