import pandas as pd
import os
import numpy as np
from sklearn.preprocessing import LabelEncoder

print("="*60)
print("🧹 DATA QUALITY CHECKER & CLEANER".center(60))
print("Built with Python & Pandas".center(60))
print("="*60)

# --- Configuration Dictionary ---
config = {
    "remove_duplicates": {"enabled": True},
    "impute_missing_values": {
        "enabled": True,
        "numeric_strategy": "median",
        "object_strategy": "mode",
        "constant_value": None # Example: 0 for numeric, 'Unknown' for object
    },
    "handle_outliers": {
        "enabled": True,
        "detection_method": "iqr", # 'iqr' or 'zscore'
        "handling_method": "cap",  # 'cap' or 'remove'
        "threshold": 1.5           # Multiplier for IQR or Z-score threshold
    },
    "convert_datatypes": {
        "enabled": True,
        "type_mapping": None, # Example: {'column_a': 'int', 'column_b': 'datetime'}
        "errors": "coerce"    # 'raise' or 'coerce'
    },
    "encode_categorical_data": {
        "enabled": True,
        "encoding_methods": None # Example: {'nominal_col': 'onehot', 'ordinal_col': 'label'}
    }
}

# --- Function Definitions ---

def get_basic_info(df):
  """Prints basic information about the DataFrame."""
  print("="*40)
  print("Basic Information")
  print("="*40)
  print(f"Total Rows: {len(df)}")
  print(f"Total Columns: {len(df.columns)}")
  print(f"\nColumn names: {df.columns.tolist()}")
  print(f"\nData Types:")
  print(df.dtypes)

def generate_quality_report(df):
  """Generates and prints a data quality report for the DataFrame."""
  print("\n" + "="*40)
  print("DATA QUALITY REPORT")
  print("="*40)
  # Missing values
  print("\nMISSING VALUES:")
  missing = df.isnull().sum()
  for column, count in missing.items():
    if count > 0:
      percentage = (count / len(df)) * 100
      print(f"{column}: {count} ({percentage:.1f}%)")
    else:
      print(f"{column}: No Missing Values.")

  # Duplicates
  duplicate_count = df.duplicated().sum()
  print(f"\nDUPLICATE ROWS: {duplicate_count}")
  if duplicate_count > 0:
    print("\nDUPLICATE ROWS FOUND:")
    print(df[df.duplicated(keep=False)])

  # Descriptive statistics for numerical columns
  print("\nNUMERICAL COLUMN STATISTICS:")
  numeric_columns = df.select_dtypes(include=np.number).columns
  if len(numeric_columns) > 0:
    for col in numeric_columns:
      print(f"\n--- Column: {col} ---")
      print(df[col].describe())
  else:
    print("No numerical columns found.")

  # Unique values and cardinality for categorical columns
  print("\nCATEGORICAL COLUMN STATISTICS:")
  categorical_columns = df.select_dtypes(include=['object', 'category']).columns
  if len(categorical_columns) > 0:
    for col in categorical_columns:
      unique_count = df[col].nunique()
      print(f"\n--- Column: {col} ---")
      print(f"Unique values count: {unique_count}")
      if unique_count < 20: # Display unique values if there are less than 20
        print("Unique values:")
        print(df[col].unique())
      else:
        print(f"Unique values (too many to display): Top 20 values\n{df[col].value_counts().head(20)}")
  else:
    print("No categorical columns found.")

def load_data(file_path):
  """Loads data from a specified file path, supporting CSV, Excel, and JSON formats."""
  file_extension = os.path.splitext(file_path)[1].lower()
  df = None
  try:
    if file_extension == '.csv':
      df = pd.read_csv(file_path)
    elif file_extension in ('.xlsx', '.xls'):
      df = pd.read_excel(file_path)
    elif file_extension == '.json':
      df = pd.read_json(file_path)
    else:
      print(f"\nERROR: Unsupported file format '{file_extension}'. Please provide a CSV, Excel, or JSON file.")
      return None

    print(f"\n 📂 ORIGINAL DATA from {file_extension.upper()} file:")
    print(df)
    return df

  except FileNotFoundError:
    print(f"\nERROR: The file '{file_path}' was not found.")
    print("Please make sure the file is in the correct directory or provide the full path.")
    return None
  except pd.errors.EmptyDataError:
    print(f"\nERROR: The file '{file_path}' is empty. Please provide a file with data.")
    return None
  except Exception as e:
    print(f"\nERROR: An unexpected error occurred while reading '{file_path}': {e}")
    return None

def impute_missing_values(df, numeric_strategy='median', object_strategy='mode', constant_value=None):
  """Imputes missing values in the DataFrame based on specified strategies."""
  df_imputed = df.copy()
  print("\n--- Missing Value Imputation ---")

  # Numeric columns imputation
  numeric_columns = df_imputed.select_dtypes(include=['float64', 'int64']).columns
  for col in numeric_columns:
    if df_imputed[col].isnull().any():
      if numeric_strategy == 'median':
        median_val = df_imputed[col].median()
        df_imputed[col] = df_imputed[col].fillna(median_val)
        print(f"Filled missing numeric column '{col}' with median: {median_val}")
      elif numeric_strategy == 'mean':
        mean_val = df_imputed[col].mean()
        df_imputed[col] = df_imputed[col].fillna(mean_val)
        print(f"Filled missing numeric column '{col}' with mean: {mean_val}")
      elif numeric_strategy == 'constant' and constant_value is not None:
        df_imputed[col] = df_imputed[col].fillna(constant_value)
        print(f"Filled missing numeric column '{col}' with constant value: {constant_value}")
      elif numeric_strategy == 'ffill':
        df_imputed[col] = df_imputed[col].ffill()
        print(f"Filled missing numeric column '{col}' using forward-fill.")
      elif numeric_strategy == 'bfill':
        df_imputed[col] = df_imputed[col].bfill()
        print(f"Filled missing numeric column '{col}' using backward-fill.")
      else:
        print(f"Warning: Unknown or unsupported numeric imputation strategy '{numeric_strategy}' for column '{col}'. Skipping.")

  # Object (text) columns imputation
  object_columns = df_imputed.select_dtypes(include=['object']).columns
  for col in object_columns:
    if df_imputed[col].isnull().any():
      if object_strategy == 'mode':
        mode_val = df_imputed[col].mode()[0] # Get the first mode if multiple exist
        df_imputed[col] = df_imputed[col].fillna(mode_val)
        print(f"Filled missing object column '{col}' with mode: {mode_val}")
      elif object_strategy == 'constant' and constant_value is not None:
        df_imputed[col] = df_imputed[col].fillna(constant_value)
        print(f"Filled missing object column '{col}' with constant value: {constant_value}")
      elif object_strategy == 'ffill':
        df_imputed[col] = df_imputed[col].ffill()
        print(f"Filled missing object column '{col}' using forward-fill.")
      elif object_strategy == 'bfill':
        df_imputed[col] = df_imputed[col].bfill()
        print(f"Filled missing object column '{col}' using backward-fill.")
      else:
        print(f"Warning: Unknown or unsupported object imputation strategy '{object_strategy}' for column '{col}'. Skipping.")

  return df_imputed

def handle_outliers(df, detection_method='iqr', handling_method='cap', threshold=1.5):
  """Detects and handles outliers in numerical columns."""
  df_outlier_handled = df.copy()
  print("\n--- Outlier Detection and Handling ---")

  numeric_columns = df_outlier_handled.select_dtypes(include=np.number).columns

  for col in numeric_columns:
    # Skip if column contains non-finite values that prevent quantile calculation (e.g., all NaNs)
    if df_outlier_handled[col].isnull().all():
        print(f"Skipping outlier detection for column '{col}': Contains only missing values.")
        continue
    
    if detection_method == 'iqr':
      Q1 = df_outlier_handled[col].quantile(0.25)
      Q3 = df_outlier_handled[col].quantile(0.75)
      IQR = Q3 - Q1
      if IQR == 0: # Handle cases where IQR is zero (all values are same or only two distinct values at Q1, Q3)
          print(f"Skipping outlier detection for column '{col}': IQR is zero.")
          continue
      lower_bound = Q1 - threshold * IQR
      upper_bound = Q3 + threshold * IQR

    elif detection_method == 'zscore':
      mean = df_outlier_handled[col].mean()
      std_dev = df_outlier_handled[col].std()
      if std_dev == 0: # Avoid division by zero if all values are the same
          print(f"Skipping outlier detection for column '{col}': standard deviation is zero.")
          continue
      lower_bound = mean - threshold * std_dev
      upper_bound = mean + threshold * std_dev

    else:
      print(f"Warning: Unknown or unsupported outlier detection method '{detection_method}'. Skipping column '{col}'.")
      continue

    # Identify outliers
    outliers = (df_outlier_handled[col] < lower_bound) | (df_outlier_handled[col] > upper_bound)
    outlier_count = outliers.sum()

    if outlier_count > 0:
      if handling_method == 'cap':
        df_outlier_handled[col] = np.where(df_outlier_handled[col] < lower_bound, lower_bound, df_outlier_handled[col])
        df_outlier_handled[col] = np.where(df_outlier_handled[col] > upper_bound, upper_bound, df_outlier_handled[col])
        print(f"Capped {outlier_count} outliers in column '{col}' using {detection_method.upper()} method (threshold={threshold}).")
      elif handling_method == 'remove':
        initial_rows = len(df_outlier_handled)
        df_outlier_handled = df_outlier_handled[~outliers]
        removed_rows = initial_rows - len(df_outlier_handled)
        print(f"Removed {removed_rows} rows with outliers in column '{col}' using {detection_method.upper()} method (threshold={threshold}).")
      else:
        print(f"Warning: Unknown or unsupported outlier handling method '{handling_method}'. Skipping column '{col}'.")
    else:
      print(f"No outliers detected in column '{col}' using {detection_method.upper()} method (threshold={threshold}).")

  return df_outlier_handled

def convert_datatypes(df, type_mapping=None, errors='coerce'):
  """Converts data types of columns based on mapping or inference, handling errors."""
  df_converted = df.copy()
  print("\n--- Data Type Conversion and Validation ---")

  if type_mapping:
    for col, target_dtype in type_mapping.items():
      if col in df_converted.columns:
        try:
          if target_dtype in ['int', 'float']:
            df_converted[col] = pd.to_numeric(df_converted[col], errors=errors)
          elif target_dtype == 'datetime':
            df_converted[col] = pd.to_datetime(df_converted[col], errors=errors)
          else:
            df_converted[col] = df_converted[col].astype(target_dtype, errors=errors)
          print(f"Converted column '{col}' to '{target_dtype}'. Errors handled by '{errors}'.")
        except Exception as e:
          print(f"Error converting column '{col}' to '{target_dtype}': {e}")
      else:
        print(f"Warning: Column '{col}' not found for type conversion.")
  else:
    # Attempt to infer types for object columns
    for col in df_converted.select_dtypes(include=['object']).columns:
      # Try to convert to numeric
      original_dtype = df_converted[col].dtype
      converted_col = pd.to_numeric(df_converted[col], errors=errors)
      # Only convert if it makes sense (i.e., if original was not numeric and conversion is successful for some values)
      if not pd.api.types.is_numeric_dtype(original_dtype) and pd.api.types.is_numeric_dtype(converted_col) and converted_col.notna().any():
        df_converted[col] = converted_col
        print(f"Inferred and converted object column '{col}' to numeric. Errors handled by '{errors}'.")
        continue

      # Try to convert to datetime
      converted_col = pd.to_datetime(df_converted[col], errors=errors)
      # Check if conversion was successful for at least some values and if it's not already datetime
      if not pd.api.types.is_datetime64_any_dtype(original_dtype) and converted_col.notna().any():
        df_converted[col] = converted_col
        print(f"Inferred and converted object column '{col}' to datetime. Errors handled by '{errors}'.")

  return df_converted

def encode_categorical_data(df, encoding_methods=None):
  """Encodes categorical features using One-Hot or Label Encoding."""
  df_encoded = df.copy()
  print("\n--- Categorical Data Encoding ---")

  categorical_cols = df_encoded.select_dtypes(include=['object', 'category']).columns

  if encoding_methods is None:
    encoding_methods = {}

  for col in categorical_cols:
    if col in encoding_methods:
      if encoding_methods[col] == 'onehot':
        # Handle potential non-string values by converting to string before get_dummies
        df_encoded[col] = df_encoded[col].astype(str)
        df_encoded = pd.get_dummies(df_encoded, columns=[col], prefix=col)
        print(f"One-Hot encoded column '{col}'.")
      elif encoding_methods[col] == 'label':
        le = LabelEncoder()
        # Handle potential non-string values by converting to string before LabelEncoder
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
        print(f"Label encoded column '{col}'.")
      else:
        print(f"Warning: Unknown encoding method '{encoding_methods[col]}' for column '{col}'. Skipping.")
    else:
      # Default to One-Hot Encoding if not specified, convert to string first
      df_encoded[col] = df_encoded[col].astype(str)
      df_encoded = pd.get_dummies(df_encoded, columns=[col], prefix=col)
      print(f"One-Hot encoded column '{col}' (default).")

  return df_encoded

def clean_data(df, config):
    """Applies a series of cleaning steps to the DataFrame based on the provided configuration."""
    df_clean = df.copy()

    # 1. Remove Duplicates
    if config['remove_duplicates']['enabled']:
        before = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        after = len(df_clean)
        removed = before - after
        print(f"\n Removed {removed} duplicate row(s)")

    # 2. Fill missing values
    if config['impute_missing_values']['enabled']:
        impute_params = {k: v for k, v in config['impute_missing_values'].items() if k != 'enabled'}
        df_clean = impute_missing_values(df_clean, **impute_params)

    # 3. Handle outliers
    if config['handle_outliers']['enabled']:
        outlier_params = {k: v for k, v in config['handle_outliers'].items() if k != 'enabled'}
        df_clean = handle_outliers(df_clean, **outlier_params)

    # 4. Convert data types
    if config['convert_datatypes']['enabled']:
        convert_params = {k: v for k, v in config['convert_datatypes'].items() if k != 'enabled'}
        df_clean = convert_datatypes(df_clean, **convert_params)

    # 5. Encode categorical data
    if config['encode_categorical_data']['enabled']:
        encode_params = {k: v for k, v in config['encode_categorical_data'].items() if k != 'enabled'}
        df_clean = encode_categorical_data(df_clean, **encode_params)

    return df_clean

# --- Main Script Flow ---

# Ask user for file
file_name = input("\nEnter file name (example: data.csv, data.xlsx, data.json): ").strip()

# Read data using the new load_data function
df = load_data(file_name)

# Proceed only if data was loaded successfully
if df is not None:
  # Call refactored functions for initial report
  get_basic_info(df)
  generate_quality_report(df)

  print("\n"+"="*40)
  print("CLEANING DATA")
  print("="*40)

  # Apply all cleaning steps via the new clean_data function
  df_clean = clean_data(df, config)

  print("\n"+"="*40)
  print("CLEANED DATA")
  print("="*40)
  print(df_clean)

  # Ask user for output file name
  output_file_name = input("\nEnter the desired name for the cleaned data file (e.g., cleaned_data.csv): ").strip()
  if not output_file_name:
      output_file_name = 'cleaned_data.csv' # Default name if user enters nothing
      print(f"No file name entered. Saving to default: '{output_file_name}'")

  # Save as CSV
  df_clean.to_csv(output_file_name, index=False)
  print(f"\n Saved to '{output_file_name}'")