"""
cleaner.py
-----------
Complete Data Quality Checker & Cleaner module.
Merges data_cleaner_backend.py + cleaner.py — all logic preserved, nothing changed.

Contains:
  - clean_dataframe()        → simple cleaner (duplicates + median/mode fill) + stats
  - generate_report()        → summary report from original vs cleaned
  - clean_data()             → full 5-step configurable pipeline
  - get_basic_info()         → structural info dict
  - generate_quality_report()→ full quality report dict
  - load_data()              → CSV / Excel / JSON loader
  - save_data()              → CSV saver
  + all individual step functions (impute, outliers, convert, encode)
"""

import os
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder


# ---------------------------------------------------------------------------
# Default Configuration  (used by clean_data())
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "remove_duplicates": {"enabled": True},
    "impute_missing_values": {
        "enabled": True,
        "numeric_strategy": "median",
        "object_strategy": "mode",
        "constant_value": None,
    },
    "handle_outliers": {
        "enabled": True,
        "detection_method": "iqr",
        "handling_method": "cap",
        "threshold": 1.5,
    },
    "convert_datatypes": {
        "enabled": True,
        "type_mapping": None,
        "errors": "coerce",
    },
    "encode_categorical_data": {
        "enabled": True,
        "encoding_methods": None,
    },
}


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_data(file_path: str) -> pd.DataFrame | None:
    """
    Loads data from a CSV, Excel, or JSON file.
    Returns a DataFrame on success, or raises on failure.
    """
    file_extension = os.path.splitext(file_path)[1].lower()

    try:
        if file_extension == ".csv":
            df = pd.read_csv(file_path)
        elif file_extension in (".xlsx", ".xls"):
            df = pd.read_excel(file_path)
        elif file_extension == ".json":
            df = pd.read_json(file_path)
        else:
            raise ValueError(
                f"Unsupported file format '{file_extension}'. "
                "Please provide a CSV, Excel, or JSON file."
            )
        return df

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except pd.errors.EmptyDataError:
        raise ValueError(f"The file '{file_path}' is empty.")


# ---------------------------------------------------------------------------
# Reporting / Inspection
# ---------------------------------------------------------------------------

def get_basic_info(df: pd.DataFrame) -> dict:
    """
    Returns basic structural information about the DataFrame.

    Keys: total_rows, total_columns, column_names, dtypes
    """
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "column_names": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
    }


def generate_quality_report(df: pd.DataFrame) -> dict:
    """
    Generates a full data quality report for the DataFrame.

    Keys: missing_values, duplicate_count, duplicate_rows,
          numeric_stats, categorical_stats
    """
    report = {}

    # --- Missing values ---
    missing_info = {}
    missing = df.isnull().sum()
    for col, count in missing.items():
        missing_info[col] = {
            "count": int(count),
            "percentage": round((count / len(df)) * 100, 1) if count > 0 else 0.0,
        }
    report["missing_values"] = missing_info

    # --- Duplicates ---
    duplicate_count = int(df.duplicated().sum())
    report["duplicate_count"] = duplicate_count
    report["duplicate_rows"] = (
        df[df.duplicated(keep=False)] if duplicate_count > 0 else pd.DataFrame()
    )

    # --- Numeric column statistics ---
    numeric_stats = {}
    numeric_columns = df.select_dtypes(include=np.number).columns
    for col in numeric_columns:
        numeric_stats[col] = df[col].describe().to_dict()
    report["numeric_stats"] = numeric_stats

    # --- Categorical column statistics ---
    categorical_stats = {}
    categorical_columns = df.select_dtypes(include=["object", "category"]).columns
    for col in categorical_columns:
        unique_count = df[col].nunique()
        entry = {"unique_count": unique_count}
        if unique_count < 20:
            entry["unique_values"] = df[col].unique().tolist()
        else:
            entry["top_values"] = df[col].value_counts().head(20).to_dict()
        categorical_stats[col] = entry
    report["categorical_stats"] = categorical_stats

    return report


def generate_report(df_original: pd.DataFrame, df_clean: pd.DataFrame, stats: dict) -> dict:
    """
    Generates a summary report comparing original vs cleaned DataFrame.

    Keys: original_rows, cleaned_rows, duplicates_removed
    """
    report = {
        "original_rows": len(df_original),
        "cleaned_rows": len(df_clean),
        "duplicates_removed": stats["duplicates_removed"],
    }
    return report


# ---------------------------------------------------------------------------
# Simple Cleaner  (used by api.py via clean_dataframe)
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame):
    """
    Clean the dataframe.

    Steps:
      1. Remove duplicate rows
      2. Fill numeric columns with median
      3. Fill text columns with mode

    Returns:
      df_clean  — cleaned DataFrame
      stats     — dict with duplicates_removed, numeric_filled, text_filled
    """
    df_clean = df.copy()
    stats = {
        "duplicates_removed": 0,
        "numeric_filled": {},
        "text_filled": {},
    }

    # Remove duplicates
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    after = len(df_clean)
    stats["duplicates_removed"] = before - after

    # Fill numeric with median
    numeric_cols = df_clean.select_dtypes(include=["float64", "int64"]).columns
    for col in numeric_cols:
        if df_clean[col].isnull().any():
            median_val = df_clean[col].median()
            df_clean[col] = df_clean[col].fillna(median_val)
            stats["numeric_filled"][col] = float(median_val)

    # Fill text with mode
    text_cols = df_clean.select_dtypes(include=["object"]).columns
    for col in text_cols:
        if df_clean[col].isnull().any():
            mode_val = (
                df_clean[col].mode()[0]
                if not df_clean[col].mode().empty
                else "Unknown"
            )
            df_clean[col] = df_clean[col].fillna(mode_val)
            stats["text_filled"][col] = mode_val

    return df_clean, stats


# ---------------------------------------------------------------------------
# Individual Cleaning Steps  (used by clean_data())
# ---------------------------------------------------------------------------

def impute_missing_values(
    df: pd.DataFrame,
    numeric_strategy: str = "median",
    object_strategy: str = "mode",
    constant_value=None,
) -> pd.DataFrame:
    """
    Imputes missing values in the DataFrame based on specified strategies.

    Numeric strategies : 'median', 'mean', 'constant', 'ffill', 'bfill'
    Object strategies  : 'mode', 'constant', 'ffill', 'bfill'
    """
    df_imputed = df.copy()

    # --- Numeric columns ---
    numeric_columns = df_imputed.select_dtypes(include=["float64", "int64"]).columns
    for col in numeric_columns:
        if df_imputed[col].isnull().any():
            if numeric_strategy == "median":
                df_imputed[col] = df_imputed[col].fillna(df_imputed[col].median())
            elif numeric_strategy == "mean":
                df_imputed[col] = df_imputed[col].fillna(df_imputed[col].mean())
            elif numeric_strategy == "constant" and constant_value is not None:
                df_imputed[col] = df_imputed[col].fillna(constant_value)
            elif numeric_strategy == "ffill":
                df_imputed[col] = df_imputed[col].ffill()
            elif numeric_strategy == "bfill":
                df_imputed[col] = df_imputed[col].bfill()

    # --- Object (text) columns ---
    object_columns = df_imputed.select_dtypes(include=["object"]).columns
    for col in object_columns:
        if df_imputed[col].isnull().any():
            if object_strategy == "mode":
                df_imputed[col] = df_imputed[col].fillna(df_imputed[col].mode()[0])
            elif object_strategy == "constant" and constant_value is not None:
                df_imputed[col] = df_imputed[col].fillna(constant_value)
            elif object_strategy == "ffill":
                df_imputed[col] = df_imputed[col].ffill()
            elif object_strategy == "bfill":
                df_imputed[col] = df_imputed[col].bfill()

    return df_imputed


def handle_outliers(
    df: pd.DataFrame,
    detection_method: str = "iqr",
    handling_method: str = "cap",
    threshold: float = 1.5,
) -> pd.DataFrame:
    """
    Detects and handles outliers in numerical columns.

    Detection  : 'iqr' or 'zscore'
    Handling   : 'cap' or 'remove'
    """
    df_out = df.copy()
    numeric_columns = df_out.select_dtypes(include=np.number).columns

    for col in numeric_columns:
        if detection_method == "iqr":
            Q1 = df_out[col].quantile(0.25)
            Q3 = df_out[col].quantile(0.75)
            IQR = Q3 - Q1
            if IQR == 0:
                continue
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

        elif detection_method == "zscore":
            mean = df_out[col].mean()
            std_dev = df_out[col].std()
            if std_dev == 0:
                continue
            lower_bound = mean - threshold * std_dev
            upper_bound = mean + threshold * std_dev

        else:
            continue

        outliers = (df_out[col] < lower_bound) | (df_out[col] > upper_bound)

        if outliers.sum() > 0:
            if handling_method == "cap":
                df_out[col] = np.where(df_out[col] < lower_bound, lower_bound, df_out[col])
                df_out[col] = np.where(df_out[col] > upper_bound, upper_bound, df_out[col])
            elif handling_method == "remove":
                df_out = df_out[~outliers]

    return df_out


def convert_datatypes(
    df: pd.DataFrame,
    type_mapping: dict | None = None,
    errors: str = "coerce",
) -> pd.DataFrame:
    """
    Converts data types of columns based on a mapping or via type inference.

    type_mapping example: {'col_a': 'int', 'col_b': 'datetime'}
    errors: 'raise' or 'coerce'
    """
    df_converted = df.copy()

    if type_mapping:
        for col, target_dtype in type_mapping.items():
            if col in df_converted.columns:
                try:
                    if target_dtype in ("int", "float"):
                        df_converted[col] = pd.to_numeric(df_converted[col], errors=errors)
                    elif target_dtype == "datetime":
                        df_converted[col] = pd.to_datetime(df_converted[col], errors=errors)
                    else:
                        df_converted[col] = df_converted[col].astype(target_dtype, errors=errors)
                except Exception:
                    pass
    else:
        for col in df_converted.select_dtypes(include=["object"]).columns:
            original_dtype = df_converted[col].dtype

            # Try numeric
            converted_col = pd.to_numeric(df_converted[col], errors=errors)
            if (
                not pd.api.types.is_numeric_dtype(original_dtype)
                and pd.api.types.is_numeric_dtype(converted_col)
                and converted_col.notna().any()
            ):
                df_converted[col] = converted_col
                continue

            # Try datetime
            converted_col = pd.to_datetime(df_converted[col], errors=errors)
            if (
                not pd.api.types.is_datetime64_any_dtype(original_dtype)
                and converted_col.notna().any()
            ):
                df_converted[col] = converted_col

    return df_converted


def encode_categorical_data(
    df: pd.DataFrame,
    encoding_methods: dict | None = None,
) -> pd.DataFrame:
    """
    Encodes categorical features using One-Hot or Label Encoding.

    encoding_methods example: {'nom_col': 'onehot', 'ord_col': 'label'}
    Columns not listed default to One-Hot Encoding.
    """
    df_encoded = df.copy()
    categorical_cols = df_encoded.select_dtypes(include=["object", "category"]).columns

    if encoding_methods is None:
        encoding_methods = {}

    for col in categorical_cols:
        method = encoding_methods.get(col, "onehot")

        if method == "onehot":
            df_encoded[col] = df_encoded[col].astype(str)
            df_encoded = pd.get_dummies(df_encoded, columns=[col], prefix=col)
        elif method == "label":
            le = LabelEncoder()
            df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))

    return df_encoded


# ---------------------------------------------------------------------------
# Full Configurable Pipeline  (used when you need all 5 steps)
# ---------------------------------------------------------------------------

def clean_data(df: pd.DataFrame, config: dict = DEFAULT_CONFIG) -> pd.DataFrame:
    """
    Applies all enabled cleaning steps to the DataFrame in order:
        1. Remove duplicates
        2. Impute missing values
        3. Handle outliers
        4. Convert data types
        5. Encode categorical data

    Pass a custom config dict to override defaults.
    Returns the cleaned DataFrame.
    """
    df_clean = df.copy()

    # 1. Remove Duplicates
    if config["remove_duplicates"]["enabled"]:
        df_clean = df_clean.drop_duplicates()

    # 2. Impute Missing Values
    if config["impute_missing_values"]["enabled"]:
        impute_params = {
            k: v for k, v in config["impute_missing_values"].items() if k != "enabled"
        }
        df_clean = impute_missing_values(df_clean, **impute_params)

    # 3. Handle Outliers
    if config["handle_outliers"]["enabled"]:
        outlier_params = {
            k: v for k, v in config["handle_outliers"].items() if k != "enabled"
        }
        df_clean = handle_outliers(df_clean, **outlier_params)

    # 4. Convert Data Types
    if config["convert_datatypes"]["enabled"]:
        convert_params = {
            k: v for k, v in config["convert_datatypes"].items() if k != "enabled"
        }
        df_clean = convert_datatypes(df_clean, **convert_params)

    # 5. Encode Categorical Data
    if config["encode_categorical_data"]["enabled"]:
        encode_params = {
            k: v for k, v in config["encode_categorical_data"].items() if k != "enabled"
        }
        df_clean = encode_categorical_data(df_clean, **encode_params)

    return df_clean


# ---------------------------------------------------------------------------
# Save Helper
# ---------------------------------------------------------------------------

def save_data(df: pd.DataFrame, output_path: str = "cleaned_data.csv") -> str:
    """
    Saves the DataFrame to a CSV file.
    Returns the resolved output path.
    """
    if not output_path:
        output_path = "cleaned_data.csv"
    df.to_csv(output_path, index=False)
    return output_path