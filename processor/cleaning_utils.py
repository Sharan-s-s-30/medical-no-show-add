# cleaning_utils.py: functions to parse dates, fix ages, map booleans, etc.
import pandas as pd
from datetime import datetime

def load_csv(path: str) -> pd.DataFrame:
    """
    Read the CSV from `path` into a DataFrame.
    """
    df = pd.read_csv(path)
    return df

def parse_dates(df: pd.DataFrame,
                sched_col: str = "ScheduledDay",
                app_col: str = "AppointmentDay") -> pd.DataFrame:
    """
    Convert two columns of strings into datetime type.
    """
    df[sched_col] = pd.to_datetime(df[sched_col])
    df[app_col]   = pd.to_datetime(df[app_col])
    return df

def clip_age(df: pd.DataFrame,
             col: str = "Age",
             min_age: int = 0,
             max_age: int = 110) -> pd.DataFrame:
    """
    Replace negative or too‐large ages with min/max.
    """
    df[col] = df[col].clip(lower=min_age, upper=max_age)
    return df

def boolify_column(df: pd.DataFrame,
                   col: str,
                   true_values: list = ["Yes"],
                   false_values: list = ["No"]) -> pd.DataFrame:
    """
    Map “Yes”/“No” or 1/0 in `col` → True/False.
    """
    df[col] = df[col].apply(lambda x: x in true_values)
    return df

def compute_wait_days(df: pd.DataFrame,
                      sched_col: str = "ScheduledDay",
                      app_col: str = "AppointmentDay",
                      new_col: str = "WaitDays") -> pd.DataFrame:
    """
    Add a column = days between scheduled & appointment.
    """
    df[new_col] = (df[app_col] - df[sched_col]).dt.days
    return df

def normalize_neighborhood(df: pd.DataFrame,
                           col: str = "Neighbourhood") -> pd.DataFrame:
    """
    Strip whitespace and lower‐case all neighbourhood names.
    """
    df[col] = df[col].str.strip().str.lower()
    return df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all cleaning steps in sequence on the DataFrame.
    """
    df = parse_dates(df)
    df = clip_age(df)
    df = boolify_column(df, col="No-show", true_values=["Yes"], false_values=["No"])
    df = normalize_neighborhood(df)
    df = compute_wait_days(df)
    return df
