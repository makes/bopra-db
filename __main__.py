import os
import glob
import pandas as pd
import sqlite3
from nirs import load_nirs
from fhdb_dtypes import FHDB_DTYPES, FHDB_BOOL_T_F
from fhdb_dtypes import CALC_DTYPES

BOPRA_DB_VERSION = "v2024-001"

FHDB_EXPORT_FILE = "/data/bopra/source_data/fhdb_export.xlsx"
DERIVED_QUANTITIES_FILE = "/data/bopra/source_data/cde_main202403021535.csv"
NIRS_RAW_DIR = "/data/bopra/source_data/nirs_raw"
NIRS_AMEND_DIR = "/data/bopra/source_data/nirs_amend"

OUTPUT_DIR = "/data/bopra/db_release"
OUTPUT_LEGACY_CSV = f"{OUTPUT_DIR}/bopra_202403110610.csv"
OUTPUT_SQLITE = f"{OUTPUT_DIR}/bopra_{BOPRA_DB_VERSION}.sqlite"
OUTPUT_DIR_PARQUET = f"{OUTPUT_DIR}/bopra_{BOPRA_DB_VERSION}.parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR_PARQUET, exist_ok=True)

# ============================================================================#
# Load FHDB export
# ----------------
# - master record of enrolled cases
# - lots of fields with data quality issues
# ============================================================================#
df_fhdb = pd.read_excel(FHDB_EXPORT_FILE, na_values=["-", "?"], dtype_backend="pyarrow")

for col, dtype in FHDB_DTYPES.items():
    if col in df_fhdb.columns:
        df_fhdb[col] = df_fhdb[col].astype(dtype)

for col in FHDB_BOOL_T_F:
    df_fhdb[col] = df_fhdb[col].map({"t": True, "f": False}).astype("bool[pyarrow]")

df_fhdb = df_fhdb.drop("doc", axis=1)
df_fhdb = df_fhdb.drop("doc.1", axis=1)
df_fhdb = df_fhdb.drop("hcm", axis=1)
df_fhdb = df_fhdb.drop("pic", axis=1)
df_fhdb = df_fhdb.drop("eh", axis=1)
df_fhdb = df_fhdb.drop("patient_name", axis=1)
df_fhdb = df_fhdb.drop("case_id", axis=1)
df_fhdb = df_fhdb.drop("address", axis=1)
df_fhdb = df_fhdb.drop("address.1", axis=1)
df_fhdb = df_fhdb.drop("rc_immutable", axis=1)
df_fhdb = df_fhdb.drop("kirjaaja", axis=1)
df_fhdb = df_fhdb.drop("alarm", axis=1)
df_fhdb = df_fhdb.drop("patient", axis=1)
df_fhdb = df_fhdb.drop("rc_id", axis=1)
df_fhdb = df_fhdb.drop("alarm_id", axis=1)
df_fhdb = df_fhdb.drop("alarm_id.1", axis=1)
df_fhdb = df_fhdb.drop("alarm_immutable", axis=1)
df_fhdb = df_fhdb.drop("alarm_extra_alarm_immutable", axis=1)
df_fhdb = df_fhdb.drop("patient_id", axis=1)
df_fhdb = df_fhdb.drop("patient_id.1", axis=1)
df_fhdb = df_fhdb.drop("patient_id_duplicate", axis=1)
df_fhdb = df_fhdb.drop("contact_et_misc", axis=1)
df_fhdb = df_fhdb.drop("r_id", axis=1)
df_fhdb = df_fhdb.drop("defib_print", axis=1)
df_fhdb = df_fhdb.drop("comments", axis=1)
df_fhdb = df_fhdb.drop("consent_detail", axis=1)
df_fhdb = df_fhdb.drop("research", axis=1)
df_fhdb = df_fhdb.drop("rc_archiver", axis=1)
df_fhdb = df_fhdb.drop("rc_predecessor", axis=1)
df_fhdb = df_fhdb.drop("rc_active", axis=1)
df_fhdb = df_fhdb.drop("patient_rank", axis=1)
df_fhdb = df_fhdb.drop("p_rank", axis=1)
df_fhdb = df_fhdb.drop("code", axis=1)
df_fhdb = df_fhdb.drop("code.1", axis=1)
df_fhdb = df_fhdb.drop("kunta", axis=1)
df_fhdb = df_fhdb.drop("shp", axis=1)
df_fhdb = df_fhdb.drop("adverse_event_type", axis=1)
df_fhdb = df_fhdb.drop("adverse_reported", axis=1)
df_fhdb = df_fhdb.drop("kotikunta", axis=1)
df_fhdb = df_fhdb.drop("unit", axis=1)
df_fhdb = df_fhdb.drop("birthyear", axis=1)
df_fhdb = df_fhdb.drop("saku", axis=1)
df_fhdb = df_fhdb.drop("pat_rec_archived", axis=1)
df_fhdb = df_fhdb.drop("ssid_archived", axis=1)
df_fhdb = df_fhdb.drop("consultation", axis=1)
df_fhdb = df_fhdb.drop("adverse_detail", axis=1)

# timestamps to delete
df_fhdb = df_fhdb.drop("kirjattu", axis=1)
df_fhdb = df_fhdb.drop("pvm_klo", axis=1)
df_fhdb = df_fhdb.drop("rc_stamp", axis=1)
df_fhdb = df_fhdb.drop("t_atscene.1", axis=1)
df_fhdb = df_fhdb.drop("t_atpatient.1", axis=1)
df_fhdb = df_fhdb.drop("t_transport.1", axis=1)
df_fhdb = df_fhdb.drop("t_athospital.1", axis=1)
df_fhdb = df_fhdb.drop("evy_alarm", axis=1)
df_fhdb = df_fhdb.drop("evy_atscene", axis=1)
# t_time ~= t_alarm; t_alarm has better data quality
df_fhdb = df_fhdb.drop("t_time", axis=1)


# ============================================================================#
# Timestamp handling
# ------------------
# - convert datetimes to offsets from t_ref in nanosec (int64[pyarrow] type)
# - reference timestamp t_ref is the alert time of the physician-staffed
#   unit (t_alarm)
# ============================================================================#
FHDB_DATETIME_FMT = "%Y-%m-%d %H:%M:%S%z"

# reference timestamp
df_fhdb["t_ref"] = pd.to_datetime(df_fhdb.t_alarm, format=FHDB_DATETIME_FMT, utc=True)
df_fhdb = df_fhdb.drop("t_alarm", axis=1)

# list of timestamp columns to convert
dt_cols = [
    "ane_stamp",  # anaesthesia start
    "poc1_stamp",  # blood analysis 1
    "poc2_stamp",  # blood analysis 2
    "poc3_stamp",  # blood analysis 3
    "r_t_arrest",  # cardiac arrest
    "r_t_trosc",  # temporary rosc
    "r_t_prosc",  # permanent rosc
    "t_call",  # emergency call
    "t_ontheway",  # embarkation
    "t_atscene",  # arrival at scene
    "t_atpatient",  # patient encounter
    "t_transport",  # transport start
    "t_athospital",  # arrival to care facility
    "t_end",  # end of mission
    "t_available",  # unit becomes available
    "saku_alarm",  # ems alert
    "saku_atscene",  # ems arrival at scene
]

for col in dt_cols:
    # parse datetime
    c = pd.to_datetime(df_fhdb[col], format=FHDB_DATETIME_FMT, utc=True)
    # convert to interval
    df_fhdb[col] = (c - df_fhdb.t_ref).astype("int64[pyarrow]")

# ============================================================================#
# Date handling
# -------------
# - convert dates to offsets from t_ref in days (int64[pyarrow] type)
# - discharge_date => days_to_discharge
# - dod            => days_to_death
# ============================================================================#
FHDB_DATE_FMT = "ISO8601"

# date reference point
date_ref = df_fhdb.t_ref.dt.tz_convert(tz="Europe/Helsinki").dt.floor("D")

# dates - convert to integer days
date_cols = {"dod": "days_to_death", "discharge_date": "days_to_discharge"}

for src_col, dest_col in date_cols.items():
    # parse date
    c = pd.to_datetime(
        df_fhdb[src_col], format=FHDB_DATE_FMT, utc=False
    ).dt.tz_localize(tz="Europe/Helsinki")
    # convert to days
    df_fhdb[dest_col] = (c - date_ref).dt.days.astype("int64[pyarrow]")
    # drop source column
    df_fhdb = df_fhdb.drop(src_col, axis=1)

# ============================================================================#
# Create new surrogate for CID
# - cannot be retraced to true case ID
# ============================================================================#
df_fhdb.insert(loc=0, column="case_id", value=df_fhdb.index.astype("int64[pyarrow]"))

# ============================================================================#
# Get precalculated quantities
# - perform these in analysis code using time series data?
# ============================================================================#
df_derived = pd.read_csv(
    DERIVED_QUANTITIES_FILE, sep=";", dtype=CALC_DTYPES, dtype_backend="pyarrow"
)
df_derived = df_derived.rename(columns={"case_id": "CID"})

df_derived = df_derived.drop("base", axis=1)
df_derived = df_derived.drop("delta", axis=1)
df_derived = df_derived.drop("above_baseline", axis=1)
df_derived = df_derived.drop("comment", axis=1)

df_derived = df_fhdb[["CID", "case_id"]].merge(df_derived, on="CID", how="right")
df_derived = df_derived.drop("CID", axis=1)

# ============================================================================#
# Incorporate time series data
# ============================================================================#
NIRS_DTYPES = {
    "CID": "int64[pyarrow]",
    "rSO2": "float64[pyarrow]",
    "Mark": "bool[pyarrow]",
    "Bad_rSO2_auto": "bool[pyarrow]",
    "Bad_rSO2_manual": "bool[pyarrow]",
}

df_nirs = None
d_info = {"CID": [], "start": [], "mark": [], "end": []}
for cid in df_fhdb["CID"]:
    raw = glob.glob(f"{NIRS_RAW_DIR}/*{cid}*.csv")
    amend = glob.glob(f"{NIRS_AMEND_DIR}/nirs_{cid}_a2.csv")
    if len(raw) == 0 and len(amend) != 0:
        print(f"no raw data for {cid}")
    if len(raw) != 0 and len(amend) == 0:
        print(f"no amend data for {cid}")
    if len(raw) != 0:
        d, t_start, t_mark, t_end = load_nirs(raw, amend)
        d.insert(loc=0, column="CID", value=cid)
        if df_nirs is None:
            df_nirs = d
        else:
            df_nirs = pd.concat([df_nirs, d])
        d_info["CID"].append(cid)
        d_info["start"].append(t_start)
        d_info["mark"].append(t_mark)
        d_info["end"].append(t_end)
df_nirs_info = pd.DataFrame(d_info)
assert df_nirs is not None

for col, dtype in NIRS_DTYPES.items():
    if col in df_nirs.columns:
        df_nirs[col] = df_nirs[col].astype(dtype)

# mark is stored in nirs_info so this col is now unnecessary
df_nirs = df_nirs.drop("Mark", axis=1)

# apply surrogate id and drop true id
df_nirs = df_fhdb[["CID", "case_id", "t_ref"]].merge(df_nirs, on="CID", how="right")
df_nirs = df_nirs.drop("CID", axis=1)
df_nirs_info = df_fhdb[["CID", "case_id", "t_ref"]].merge(
    df_nirs_info, on="CID", how="right"
)
df_nirs_info = df_nirs_info.drop("CID", axis=1)

# convert timestamps to intervals
df_nirs.insert(
    loc=0,
    column="time",
    value=(df_nirs["timestamp"] - df_nirs["t_ref"]).astype("int64[pyarrow]"),
)
df_nirs = df_nirs.drop("timestamp", axis=1)
df_nirs = df_nirs.drop("t_ref", axis=1)
df_nirs_info["start"] = (df_nirs_info.start - df_nirs_info.t_ref).astype(
    "int64[pyarrow]"
)
df_nirs_info["mark"] = (df_nirs_info.mark - df_nirs_info.t_ref).astype("int64[pyarrow]")
df_nirs_info["end"] = (df_nirs_info.end - df_nirs_info.t_ref).astype("int64[pyarrow]")
df_nirs_info = df_nirs_info.drop("t_ref", axis=1)

df_nirs = df_nirs.rename(
    columns={
        "rSO2": "rso2",
        "Bad_rSO2_auto": "bad_rso2_auto",
        "Bad_rSO2_manual": "bad_rso2_manual",
    }
)

# ============================================================================#
# Finalize and write output
# ============================================================================#

# drop original true case id
df_fhdb = df_fhdb.drop("CID", axis=1)
# drop reference time col
df_fhdb = df_fhdb.drop("t_ref", axis=1)

# Write to SQLite
with sqlite3.connect(OUTPUT_SQLITE) as conn:
    df_fhdb.to_sql("fhdb", conn, index=False, if_exists="replace")
    df_derived.to_sql("derived_quantities", conn, index=False, if_exists="replace")
    df_nirs.to_sql("nirs", conn, index=False, if_exists="replace")
    df_nirs_info.to_sql("nirs_info", conn, index=False, if_exists="replace")

# Write to Parquet
df_fhdb.to_parquet(os.path.join(OUTPUT_DIR_PARQUET, "fhdb.parquet"))
df_derived.to_parquet(os.path.join(OUTPUT_DIR_PARQUET, "derived_quantities.parquet"))
df_nirs.to_parquet(os.path.join(OUTPUT_DIR_PARQUET, "nirs.parquet"))
df_nirs_info.to_parquet(os.path.join(OUTPUT_DIR_PARQUET, "nirs_info.parquet"))

# Write to CSV
df_merged = df_derived.merge(df_fhdb, on="case_id", how="outer")
df_merged.to_csv(OUTPUT_LEGACY_CSV, index=False)
