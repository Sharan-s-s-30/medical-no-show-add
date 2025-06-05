# cleaning_utils.py

import pandas as pd
from io import BytesIO

def load_csv(path: str) -> pd.DataFrame:
    """Read the CSV from `path` into a DataFrame."""
    return pd.read_csv(path)

def load_csv_from_bytes(raw_bytes: bytes) -> pd.DataFrame:
    """Load CSV content from raw bytes into a pandas DataFrame."""
    return pd.read_csv(BytesIO(raw_bytes))

def normalize_and_rename(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Lower-case & underscore-ify all column names
    2. Rename known CamelCase & typo’d columns to match our schema
    """
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[ \-]", "_", regex=True)
    )
    return df.rename(columns={
        "patientid":       "patient_id",
        "appointmentid":   "appointment_id",
        "scheduledday":    "scheduled_day",
        "appointmentday":  "appointment_day",
        "hipertension":    "hypertension",
        "handcap":         "handicap",
        # "no_show" and "sms_received" already normalize correctly
    })

def parse_dates(df: pd.DataFrame,
                sched_col: str = "scheduled_day",
                app_col: str = "appointment_day") -> pd.DataFrame:
    """Parse scheduling & appointment into naive datetimes/dates."""
    # parse as UTC then drop tzinfo so both columns are tz-naive
    df[sched_col] = pd.to_datetime(df[sched_col], utc=True).dt.tz_convert(None)
    df[app_col]   = pd.to_datetime(df[app_col], utc=True).dt.tz_convert(None)
    return df

def clip_age(df: pd.DataFrame,
             col: str = "age",
             min_age: int = 0,
             max_age: int = 110) -> pd.DataFrame:
    """Clamp out-of-range ages to [min_age, max_age]."""
    df[col] = df[col].clip(lower=min_age, upper=max_age)
    return df

def normalize_neighborhood(df: pd.DataFrame,
                           col: str = "neighbourhood") -> pd.DataFrame:
    """Trim whitespace and lower-case neighborhood names."""
    df[col] = df[col].str.strip().str.lower()
    return df

def convert_flags(df):
    # numeric flags (0/1) -> bool
    for c in ["scholarship","hypertension","diabetes","alcoholism","sms_received"]:
        df[c] = df[c].astype(bool)

    # **explicit** map for no_show
    df["no_show"] = df["no_show"].map({
        "Yes":  True,
        "No":   False,
         1:     True,
         0:     False,
        True:  True,
        False: False
    }).astype(bool)

    return df

def compute_wait_days(df: pd.DataFrame,
                      sched_col: str = "scheduled_day",
                      app_col: str = "appointment_day",
                      new_col: str = "wait_days") -> pd.DataFrame:
    """Days between scheduling and appointment."""
    df[new_col] = (df[app_col] - df[sched_col]).dt.days
    
    # Drop rows with negative wait days
    df = df[df[new_col] >= 0].copy()
    return df

def compute_scheduled_hour(df: pd.DataFrame,
                           sched_col: str = "scheduled_day",
                           new_col: str = "scheduled_hour") -> pd.DataFrame:
    """Extract the hour (0–23) from scheduled_day."""
    df[new_col] = df[sched_col].dt.hour
    return df

def compute_appointment_weekday(df: pd.DataFrame,
                                app_col: str = "appointment_day",
                                new_col: str = "appointment_weekday") -> pd.DataFrame:
    """Day of week for apt.: 0=Monday … 6=Sunday."""
    df[new_col] = df[app_col].dt.weekday
    return df

def compute_age_group(df: pd.DataFrame,
                      age_col: str = "age",
                      new_col: str = "age_group") -> pd.DataFrame:
    """Bucket age into categories: child, adult, senior."""
    def bucket(a):
        if a < 18:
            return "child"
        elif a < 65:
            return "adult"
        else:
            return "senior"
    df[new_col] = df[age_col].apply(bucket)
    return df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline:
      1. normalize_and_rename
      2. parse_dates
      3. clip_age
      4. normalize_neighborhood
      5. convert_flags
      6. compute_wait_days
      7. compute_scheduled_hour
      8. compute_appointment_weekday
      9. compute_age_group
    """
    df = normalize_and_rename(df)
    df = parse_dates(df)
    df = clip_age(df)
    df = normalize_neighborhood(df)
    df = convert_flags(df)
    df = compute_wait_days(df)
    df = compute_scheduled_hour(df)
    df = compute_appointment_weekday(df)
    df = compute_age_group(df)
    return df
