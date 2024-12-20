"""
This module handles the extraction, transformation, and loading of data from zip files.
Some of the code may be considered over-engineered for the task, but I aimed to write it
as to mimic how a real-world data pipeline could work, with each ETL step running
in different environments.

Assumptions on the data are written as comments in the main function (load_data_file).

Notes:

Robust data pipeline and the infrastructure:
  * Cloud compute (Lambda, ECS, Kubernetes) to ingest the data as raw as possible (e.g zip files)
  * Another compute function to parse the ingested data into a structured format (e.g. parquet)
  * Cloud storage (S3) to store both the ingested data (e.g. zip) and the parsed data (parquet)
  * Data Warehouse to load the structured data and perform data transformations and validations
  * Apache Airflow to orchestrate ETL pipelines and scheduling jobs
  * Good monitoring and alerting to identify issues at each step of the pipeline

Real-world scenarios to expect and handle
  * provider not sending the expected file(s) on time
  * provider changing the schema: data types or fields
  * provider changing file formats or file names
  * provider sending bad data: stale, duplicates, hardcoded/invalid values,
    outliers, missing records, invalid reference data and identifiers, etc.

How to detect them:
  * Changes in file format and schema would be detected with good error handling,
    at the ingestion step, and the ability to easily extend the code to support
    new file types and schemas.
  * Issues with the data itself to be detected downstream in a datawarehouse where
  large amount of data can be queried and analyzed easily, and validation checks
    can be added.

Data issues found:
  * Potential hardcoded 1.0 values for measure_one from Broker A (dropped)
  * 1 duplicate row found and dropped, assuming PK=broker,stock_id,date (dropped)
  * Potential outliers (untouched)

"""

import concurrent.futures
import hashlib
import io
import logging
import os
import sqlite3
import sys
import zipfile
from collections import Counter
from enum import Enum, auto
from pathlib import Path
from typing import IO, List, Optional

import numpy as np
import pandas as pd
from scipy import stats

# Set up logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Create a file handler and clear the file before writing
log_output_path = os.environ["OUTPUT_PATH"]
open(log_output_path, "w").close()
file_handler = logging.FileHandler(log_output_path, mode="a")
file_handler.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
log.addHandler(file_handler)
log.addHandler(console_handler)


class ExtractionException(Exception):
    """Exception raised for errors during the extraction process."""


class TransformationException(Exception):
    """Exception raised for errors during the transformation process."""


class ValidationException(Exception):
    """Exception raised for errors during the validation process."""


class ValidationErrorType(Enum):
    """
    Enum representing different types of validation errors during data processing.

    Attributes:
        BROKER_ISSUE: Issues related to broker data, such as unusual broker values.
        PRIMARY_KEY_ISSUE: Problems with PK constraints, such as duplicates or nulls.
        NULL_VALUE_ISSUE: Presence of null values in major fields.
        Z_SCORE_ISSUE: Potential outliers identified by z-score check.
        MEASURE_ONE_ISSUE: Catch issues where Broker A sends too many 1.0 values.
        MISSING_DATA_ISSUE: Issues related to missing data on expected business days.
    """

    BROKER_ISSUE = auto()
    PRIMARY_KEY_ISSUE = auto()
    NULL_VALUE_ISSUE = auto()
    Z_SCORE_ISSUE = auto()
    MEASURE_ONE_ISSUE = auto()
    MISSING_DATA_ISSUE = auto()


def _get_dataframe(file_handle: IO[bytes]) -> pd.DataFrame:
    """
    Extracts a CSV file from a zip archive and returns it as a pandas DataFrame.
    """

    expected_schema = {
        "id": "int64",
        "date": "object",
        "source": "string",
        "measure_one": "float64",
        "measure_two": "int64",
    }

    try:
        with zipfile.ZipFile(file_handle) as zip_ref:
            csv_files = [
                Path(f) for f in zip_ref.namelist() if f.lower().endswith(".csv")
            ]

            assert csv_files, "No CSV file found in the zip archive"
            assert (
                len(csv_files) == 1
            ), f"Multiple CSV files found in the zip archive: {csv_files}"

            csv_file = csv_files[0]
            log.debug("Reading content of %s", csv_file)
            with zip_ref.open(str(csv_file)) as file:
                df = pd.read_csv(
                    file,
                    dtype=expected_schema,
                    skip_blank_lines=True,
                )

                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", utc=True)

                # Convert numeric columns without coercing errors
                df["measure_one"] = pd.to_numeric(df["measure_one"], errors="raise")
                df["measure_two"] = pd.to_numeric(df["measure_two"], errors="raise")

                return df
    except Exception as e:
        raise ExtractionException(f"Error parsing CSV file: {str(e)}") from e


def extract(
    zip_file_path: Path, output_root: Path, output_suffix: str = "parsed"
) -> Path:
    """
    Extracts data from a zip file, processes it, and saves it as a CSV file.

    Args:
        zip_file_path (Path): The path to the input zip file.
        output_root (Path): The root directory for the output CSV file.
        output_suffix (str, optional): The suffix to append to the output file name.
            Defaults to "parsed".

    Returns:
        Path: The path to the generated CSV file.

    Raises:
        ExtractionException: If any error occurs during the extraction or
            parsing process.
    """

    extension = "csv"

    try:

        assert (
            zip_file_path.suffix.lower() == ".zip"
        ), f"Input file {zip_file_path} must have a .zip extension"

        assert zip_file_path.exists(), f"File not found: {zip_file_path}"

        with open(zip_file_path, "rb") as zip_file:
            file_handle = io.BytesIO(zip_file.read())
            df = _get_dataframe(file_handle)
            assert (
                df is not None and not df.empty
            ), f"Empty DataFrame from _get_dataframe() for zip_file_path={zip_file_path}"

            # Add static metadata columns for traceability
            file_handle.seek(0)
            df["meta_sha1_hash"] = hashlib.sha1(file_handle.read()).hexdigest()
            df["meta_filename"] = str(zip_file_path)
            df["meta_ingested_at"] = pd.Timestamp.now(tz="UTC").round("ms")

        # Create the output file path
        base_name = zip_file_path.stem
        output_file_name = f"{base_name}__{output_suffix}.{extension}"
        output_path = output_root / output_file_name

        df.to_csv(output_path, index=False)
        log.debug("Parsed CSV file with data types:\n%s", df.dtypes)
        log.info("Saved parsed CSV file to: %s", output_path)
        return output_path

    except Exception as e:
        raise ExtractionException(f"Error during extraction: {repr(e)}") from e


def transform(
    extracted_table_path: Path, output_root: Path, output_suffix: str = "transformed"
) -> Path:
    """
    Transforms the extracted CSV file into a cleaned CSV file suitable for ML.

    Args:
        extracted_table_path (Path): Path to the extracted CSV file.
        output_root (Path): The root directory for the output CSV file.
        output_suffix (str, optional): The suffix to append to the output file name.
            Defaults to "transformed".

    Returns:
        Path: The path to the generated cleaned CSV file.

    Raises:
        TransformationException: If any error occurs during the transformation process.
    """
    try:

        assert extracted_table_path.stem.endswith(
            "__parsed"
        ), f"Input file {extracted_table_path} must end with '__parsed'"

        df = pd.read_csv(extracted_table_path)
        log.debug(f"Starting transformation of {extracted_table_path}")
        log.debug(f"Initial DataFrame shape: {df.shape}")
        log.debug(f"Initial columns: {df.columns.tolist()}")

        # Filter out rows where id is null or empty
        log.debug("Filtering out rows with null or empty id")
        df = df[df["id"].notna() & (df["id"] != "")]
        log.debug(f"DataFrame shape after filtering id: {df.shape}")

        # Drop rows where measure_one is exactly 1.0 and broker is 'A'
        log.debug("Dropping rows where measure_one is exactly 1.0 and broker is 'A'")
        initial_row_count = len(df)
        df = df[~((df["measure_one"] == 1.0) & (df["source"] == "A"))]
        dropped_row_count = initial_row_count - len(df)
        log.debug(
            f"Dropped {dropped_row_count} rows where measure_one "
            "was exactly 1.0 and broker was 'A'"
        )

        # Define column mapping with desired order
        column_mapping = {
            "date": "trading_datetime",
            "id": "stock_id",
            "source": "broker",
            "measure_one": "measure_one",
            "measure_two": "measure_two",
        }
        log.debug("Renaming columns")
        df = df.rename(columns=column_mapping)
        log.debug(f"New columns: {df.columns.tolist()}")

        # Convert 'trading_datetime' to datetime
        df["trading_datetime"] = pd.to_datetime(df["trading_datetime"], utc=True)

        # Remove rows with null values in primary key columns
        primary_key_columns = ["broker", "stock_id", "trading_datetime"]
        initial_row_count = len(df)
        df = df.dropna(subset=primary_key_columns)
        dropped_row_count = initial_row_count - len(df)
        log.debug(
            f"Dropped {dropped_row_count} rows with null values in primary key columns"
        )

        # Remove duplicate rows based on primary key columns
        initial_row_count = len(df)
        df = df.drop_duplicates(subset=primary_key_columns, keep="first")
        dropped_row_count = initial_row_count - len(df)
        log.debug(
            f"Dropped {dropped_row_count} duplicate rows based on primary key columns"
        )

        # Feature engineering
        # These FE columns must be defined with the ML team
        df["stats__year"] = df["trading_datetime"].dt.year
        df["stats__month"] = df["trading_datetime"].dt.month
        df["stats__day"] = df["trading_datetime"].dt.day
        df["stats__dow"] = df["trading_datetime"].dt.dayofweek
        df["stats__is_weekend"] = df["stats__dow"].apply(lambda x: 1 if x >= 5 else 0)
        log.debug(
            f"Added feature engineering columns. New columns: {df.columns.tolist()}"
        )

        # Reorder columns to move metadata fields to the end
        main_columns = [
            "broker",
            "stock_id",
            "trading_datetime",
            "measure_one",
            "measure_two",
            "stats__year",
            "stats__month",
            "stats__day",
            "stats__dow",
            "stats__is_weekend",
        ]
        metadata_columns = ["meta_sha1_hash", "meta_filename", "meta_ingested_at"]
        df = df[main_columns + metadata_columns]
        log.debug(f"Reordered columns. Final column order: {df.columns.tolist()}")

        # Sort the DataFrame by broker (asc), stock_id (asc), and trading_datetime (desc)
        df = df.sort_values(
            ["broker", "stock_id", "trading_datetime"], ascending=[True, True, False]
        )
        log.debug("Sorted DataFrame by broker, stock_id and trading_datetime (desc)")

        # Create the output file path
        base_name = extracted_table_path.stem.replace("__parsed", "")
        output_file_name = f"{base_name}__{output_suffix}{extracted_table_path.suffix}"
        transformed_file_path = output_root / output_file_name

        df.to_csv(transformed_file_path, index=False)
        log.info(f"Saved transformed file to: {transformed_file_path}")

        return transformed_file_path

    # Broad exception not ideal, would have to be refined after testing with more data.
    except Exception as e:
        raise TransformationException(f"Error during transformation: {repr(e)}") from e


def _create_validation_df(
    error_type: ValidationErrorType, error_messages: List[str]
) -> pd.DataFrame:
    """
    Creates a standardized validation DataFrame.

    Args:
        error_type (ValidationErrorType): The type of validation error.
        error_messages (List[str]): List of error messages.

    Returns:
        pd.DataFrame: A DataFrame with standardized schema for validation errors.
    """
    if not error_messages:
        return pd.DataFrame(columns=["error_type", "error_message"])

    return pd.DataFrame(
        {
            "error_type": [error_type.name] * len(error_messages),
            "error_message": error_messages,
        }
    )


def _validate_broker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate broker data for unusual values by looking at counts per broker.

    Assumption: We expect a similar row count for each broker.

    Another method would be to maintain a hardcoded list of allowed values.
    """
    log.debug("Starting broker validation")

    broker_counts = Counter(df["broker"])
    counts = np.array(list(broker_counts.values()))
    mean_count = np.mean(counts)
    std_count = np.std(counts)
    threshold = max(float(mean_count - 2 * std_count), 1.0)

    potential_invalid_brokers = [
        broker for broker, count in broker_counts.items() if count < threshold
    ]

    error_messages = [
        f"Broker {broker} count: {broker_counts[broker]} (below threshold)"
        for broker in potential_invalid_brokers
    ]

    return _create_validation_df(
        error_type=ValidationErrorType.BROKER_ISSUE, error_messages=error_messages
    )


def _validate_primary_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate primary key constraints.
    """

    log.debug("Starting primary key validation")

    primary_key_columns = ["broker", "stock_id", "trading_datetime"]
    null_counts = df[primary_key_columns].isnull().sum()
    duplicates = df[df.duplicated(subset=primary_key_columns, keep=False)]

    error_messages = []

    if null_counts.any():
        null_details = ", ".join(
            [f"{col}: {count}" for col, count in null_counts.items() if count > 0]
        )
        error_messages.append(f"Primary key columns contain null values: {null_details}")

    if not duplicates.empty:
        duplicate_groups = duplicates.groupby(primary_key_columns).size()
        error_messages.append(
            f"Primary key constraint violated. Found {len(duplicates)} duplicate "
            f" records across {len(duplicate_groups)} groups."
        )

    return _create_validation_df(
        error_type=ValidationErrorType.PRIMARY_KEY_ISSUE, error_messages=error_messages
    )


def _validate_null_measures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assumption: Null values are problematic for both measures.
    """

    log.debug("Starting measure validation")

    measure_columns = ["measure_one", "measure_two"]
    null_rows = df[df[measure_columns].isnull().any(axis=1)]

    error_messages = []

    for _, row in null_rows.iterrows():
        null_measures = [col for col in measure_columns if pd.isnull(row[col])]
        error_messages.append(
            f"Null measure(s) {', '.join(null_measures)} found: "
            f"stock_id={row['stock_id']}, broker={row['broker']}, "
            f"trading_datetime={row['trading_datetime']}"
        )

    return _create_validation_df(
        error_type=ValidationErrorType.NULL_VALUE_ISSUE, error_messages=error_messages
    )


def _validate_outliers(df: pd.DataFrame, z_threshold: float = 5.0) -> pd.DataFrame:
    """
    Assumptions:
        * ZScore is an effective method at identifying outliers for this dataset.
        * No rolling window is required for this dataset.
    """
    log.debug("Starting outlier validation")

    measure_columns = ["measure_one", "measure_two"]
    df = df.sort_values(["broker", "stock_id", "trading_datetime"])

    error_messages = []

    for (broker, stock_id), group in df.groupby(["broker", "stock_id"]):
        for measure in measure_columns:
            z_scores = np.abs(stats.zscore(group[measure]))

            for (_, row), z_score in zip(group.iterrows(), z_scores):
                if z_score > z_threshold:
                    error_messages.append(
                        f"Potential outlier: broker={broker}, stock_id={stock_id}, "
                        f"trading_datetime={row['trading_datetime']}, "
                        f"measure={measure}, value={row[measure]}, "
                        f"z_score={z_score:.2f}"
                    )

    return _create_validation_df(
        error_type=ValidationErrorType.Z_SCORE_ISSUE, error_messages=error_messages
    )


def _validate_measure_one_exact_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check for measure_one values that are exactly 1.0 from Broker A
    which are likely data issues.
    """
    log.debug("Starting measure_one exact value validation")

    exact_one_rows = df[df["measure_one"] == 1.0]

    error_messages = []
    for _, row in exact_one_rows.iterrows():
        error_messages.append(
            f"measure_one exactly 1.0: broker={row['broker']}, "
            f"stock_id={row['stock_id']}, trading_datetime={row['trading_datetime']}"
        )

    return _create_validation_df(
        error_type=ValidationErrorType.MEASURE_ONE_ISSUE, error_messages=error_messages
    )


def _validate_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    log.debug("Starting missing data validation")

    df = df.sort_values(["broker", "stock_id", "trading_datetime"])
    error_messages = []

    # This would not be necessary if the intermediate datasets
    # could be stored as parquet instead of csv.
    if not pd.api.types.is_datetime64_any_dtype(df["trading_datetime"]):
        df["trading_datetime"] = pd.to_datetime(df["trading_datetime"], errors="coerce")

    for (broker, stock_id), group in df.groupby(["broker", "stock_id"]):
        dates = group["trading_datetime"].dt.date
        date_range = pd.date_range(start=dates.min(), end=dates.max())

        missing_dates = set(date.date() for date in date_range) - set(dates)
        missing_weekdays = sorted(
            [d for d in missing_dates if d.weekday() < 5]
        )  # Exclude weekends and sort

        for missing_date in missing_weekdays:
            error_messages.append(
                {
                    "broker": broker,
                    "stock_id": stock_id,
                    "missing_date": missing_date,
                    "message": (
                        f"Missing data for broker={broker}, stock_id={stock_id} on date:"
                        f" {missing_date}"
                    ),
                }
            )

    # Sort error messages
    sorted_error_messages = sorted(
        error_messages, key=lambda x: (x["broker"], x["stock_id"], x["missing_date"])
    )

    return _create_validation_df(
        error_type=ValidationErrorType.MISSING_DATA_ISSUE,
        error_messages=[msg["message"] for msg in sorted_error_messages],
    )


def validate(
    transformed_file_path: Path,
    output_suffix: str = "validation_results",
    raise_exception: bool = False,
) -> Path:
    """
    Validates the transformed CSV file and saves validation results.

    Args:
        transformed_file_path (Path): Path to the transformed CSV file.
        output_suffix (str, optional): The suffix to append to the output file name.
            Defaults to "validation_results".
        raise_exception (bool, optional): If True, raises a ValidationException when
            validation fails. Defaults to False.

    Raises:
        ValidationException: If any validation checks fail and raise_exception is True.

    Returns:
        Path: The path to the saved validation results CSV file.
    """

    assert transformed_file_path.stem.endswith(
        "__transformed"
    ), f"Input file {transformed_file_path} must end with '__transformed'"

    df = pd.read_csv(transformed_file_path)
    log.debug("Starting data validation")

    validation_results = pd.concat(
        [
            _validate_broker(df),
            _validate_primary_key(df),
            _validate_null_measures(df),
            _validate_outliers(df),
            _validate_measure_one_exact_value(df),
            _validate_missing_data(df),
        ],
        ignore_index=True,
    )

    # Create the validation results file path
    base_name = transformed_file_path.stem.replace("__transformed", "")
    validation_file_name = f"{base_name}__{output_suffix}.csv"
    validation_file_path = transformed_file_path.with_name(validation_file_name)

    validation_results.to_csv(validation_file_path, index=False)
    log.info(f"Saved validation results to: {validation_file_path}")

    if not validation_results.empty:
        # Count error types and format for logging
        all_error_types = [e.name for e in ValidationErrorType]
        error_type_counts = validation_results["error_type"].value_counts()

        # Sort error types by count in descending order
        sorted_error_types = sorted(
            all_error_types, key=lambda x: error_type_counts.get(x, 0), reverse=True
        )

        error_type_summary = ", ".join(
            [
                f"{error_type}: {error_type_counts.get(error_type, 0)}"
                for error_type in sorted_error_types
            ]
        )
        log.warning(f"Validation failed. Error types and counts: {error_type_summary}")

        if raise_exception:
            error_message = "\n".join(validation_results["error_message"])
            raise ValidationException(error_message)
    else:
        # If no errors, still print all error types with 0 count
        error_type_summary = ", ".join([f"{e.name}: 0" for e in ValidationErrorType])
        log.info(
            "Data validation completed successfully. Error types and counts:"
            f" {error_type_summary}"
        )

    return validation_file_path


def load(
    df_file_path: Path, table_name: str, database_file: Optional[str] = None
) -> None:
    """
    Loads the validated CSV file into a SQLite database.

    Args:
        df_file_path (Path): The path to the validated CSV file.
        table_name (str): The name of the table to create and insert data into.
        database_file (str, optional): The path to the SQLite database file.
            If None, an in-memory database is used. Defaults to None.

    Returns:
        None
    """
    # Read the CSV file
    df = pd.read_csv(df_file_path)

    # Check if the DataFrame is empty
    if df.empty:
        log.info(f"The file at {df_file_path} is empty. Skipping load operation.")
        return None

    # Connect and write to the database
    with sqlite3.connect(database_file if database_file else ":memory:") as conn:
        # Create the table based on the DataFrame schema
        df.to_sql(table_name, conn, index=False, if_exists="replace")

    log.info(
        f"Data loaded into table: {table_name} -- "
        f"Number of rows: {len(df)} -- "
        f"Columns: {', '.join(df.columns)} -- "
        f"Database: {'In-memory' if not database_file else f'File: {database_file}'}"
    )

    return None


def load_data_file(filename: str, database_file: Optional[str] = None) -> str:
    """
    Loads data from a zip file, processes, and prepares it for downstream ML tasks.

    This function performs the following steps:
    1. Extracts and parses data from the input zip file.
    2. Transforms the data for Machine Learning purposes.
    3. Validates the transformed data.
    4. Loads both the transformed data and validation results into SQLite tables.

    Each step write an intermediate file to disk which is picked up by the next step.
    This is to mimic how a real-world data pipeline could work, with each step running
    in a separate environment and R/W to a datalake, e.g. S3.

    Args:
        filename (str): The name of the input zip file.
        database_file (str | None, optional): The path to the SQLite database file.
            If None, an in-memory database is used. Defaults to None.

    Returns:
        str: The name of the transformed table in the SQLite database.
    """
    file_path, output_root = Path(filename), Path(__file__).parent

    # 1. Extract and parse the data into a predefined schema
    # Assumptions:
    # * The zip file contains exactly one CSV file
    # * The CSV file follows a stable schema
    # * Dates are formatted as YYYY-MM-DD
    # * 'measure_one' and 'measure_two' are numeric fields
    # Note: The extraction can be extended using OOP and different implementations
    #       of the _get_dataframe() method, to handle various file types and schemas.
    extracted_table_path = extract(zip_file_path=file_path, output_root=output_root)

    # 2. Transform the data for machine learning
    # Assumptions:
    # * Broker A may send incorrect 'measure_one' values hardcoded to 1.0, which are dropped
    # * Duplicate rows are removed, assuming the primary key is (broker, stock_id, date)
    # * 'measure_two' remains unchanged for ML purposes, though normalization is possible
    # * Columns are renamed and ordered according to the ML team's standards
    transformed_file_path = transform(extracted_table_path, output_root=output_root)

    # 3. Validate the data and identify issues
    # Validation checks are defined in the ValidationErrorType enum.
    # Assumptions:
    # * 'measure_one' can have negative values
    # * Null values are not allowed in 'measure_one' and 'measure_two'
    # * Z-score is a good method to detect outliers for this dataset
    # * Each broker should have a similar number of records
    # * Daily records are expected for each stock_id from each broker, though
    #   this may not be realistic
    validation_results = validate(transformed_file_path, raise_exception=False)

    # 4. Load the transformed and validation datasets concurrently
    # This approach is more complex than necessary for this task but would be efficient for multiple tables
    transformed_table = "stock_loan__transformed"
    validation_table = "stock_loan__validation_results"
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_transformed = executor.submit(
            load,
            df_file_path=transformed_file_path,
            table_name=transformed_table,
            database_file=database_file,
        )
        future_validation = executor.submit(
            load,
            df_file_path=validation_results,
            table_name=validation_table,
            database_file=database_file,
        )

        # Wait for both operations to complete
        concurrent.futures.wait([future_transformed, future_validation])

    return transformed_table


if __name__ == "__main__":

    # Set database_file to None to use an in-memory database
    out_table = load_data_file(
        filename="dc-test-file-ingestion.zip", database_file=None  # "db.sqlite"
    )

    log.info(f"Transformed table: {out_table}")
