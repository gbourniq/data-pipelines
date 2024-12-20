"""
CORD-19 Data Processing

Ingests, processes, and stores data from the COVID-19 Open Research Dataset (CORD-19).

Tailored for extracting country info from titles and storing author emails
for future country mapping.

Features:
- Extracts data from zip files containing JSON research papers
- Transforms data, focusing on titles, authors, and full text
- Loads processed data into SQLite database
"""

import hashlib
import io
import json
import logging
import os
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, TypedDict

import pandas as pd

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


class Author(TypedDict):
    first: str
    middle: List[str]
    last: str
    suffix: str
    email: Optional[str]
    affiliation: Any


class Metadata(TypedDict):
    title: str
    authors: List[Author]


class RefSpan(TypedDict):
    start: int
    end: int
    mention: str
    ref_id: str


class BodyText(TypedDict):
    text: str
    cite_spans: List[RefSpan]
    section: str
    ref_spans: List[RefSpan]


class RefEntry(TypedDict):
    text: str
    type: str


class BibEntryAuthor(TypedDict):
    first: str
    middle: List[str]
    last: str
    suffix: str


class BibEntryOtherIds(TypedDict):
    DOI: List[str]


class BibEntry(TypedDict):
    title: str
    authors: List[BibEntryAuthor]
    year: str
    venue: str
    volume: str
    issn: str
    pages: str
    other_ids: BibEntryOtherIds


class Paper(TypedDict):
    paper_id: str
    metadata: Metadata
    body_text: List[BodyText]
    ref_entries: Dict[str, RefEntry]
    back_matter: List[Any]
    bib_entries: Dict[str, BibEntry]


class ExtractionError(Exception):
    """Exception raised for errors during the extraction process."""


class TransformationError(Exception):
    """Exception raised for errors during the transformation process."""


def _get_dataframe(file_handle: IO[bytes]) -> pd.DataFrame:
    """
    Extracts JSON files from a zip archive and returns them in a single pandas
    DataFrame.

    Args:
        file_handle (IO[bytes]): An IO object containing the zip file data.

    Returns:
        pd.DataFrame: A pandas DataFrame with flattened paper data.

    Raises:
        ExtractionError: If there's an error parsing the JSON files.
    """

    def _flatten_paper_json(paper_data: Paper) -> Dict:
        """
        Flatten a single paper's JSON data into a dictionary suitable for a
        DataFrame row.
        """
        flattened = {
            "paper_id": paper_data["paper_id"],
            "metadata": json.dumps(paper_data["metadata"]),
            "body_text": json.dumps(paper_data["body_text"]),
            "ref_entries": json.dumps(paper_data["ref_entries"]),
            "back_matter": json.dumps(paper_data["back_matter"]),
            "bib_entries": json.dumps(paper_data["bib_entries"]),
        }
        return flattened

    try:
        with zipfile.ZipFile(file_handle) as zip_ref:
            json_files = [f for f in zip_ref.namelist() if f.lower().endswith(".json")]

            assert json_files, "No JSON file found in the zip archive"

            data: List[Dict] = []
            for json_file in json_files:
                with zip_ref.open(json_file) as file:
                    paper_data: Paper = json.loads(file.read().decode("utf-8"))
                    flattened_data = _flatten_paper_json(paper_data)

                    # Add the JSON file name to the flattened data
                    flattened_data["json_filename"] = json_file

                    # Assert that the file name without extension matches the paper_id
                    file_name_without_ext = Path(json_file).name.split(".")[0]
                    if file_name_without_ext != flattened_data["paper_id"]:
                        log.error(
                            f"File name '{file_name_without_ext}' does not match"
                            f" paper_id '{flattened_data['paper_id']}' for file"
                            f" {json_file}. Skip parsing."
                        )
                        continue
                    data.append(flattened_data)
                    log.debug(f"Extracted {json_file}")

            df = pd.DataFrame(data)
            return df
    except Exception as e:
        raise ExtractionError(f"Error parsing CSV file: {str(e)}") from e


def extract(
    zip_file_path: Path, output_root: Path, output_suffix: str = "parsed"
) -> Path:
    """
    Extracts data from a zip file, parses it, and saves it as a CSV file.

    Args:
        zip_file_path (Path): The path to the input zip file.
        output_root (Path): The root directory for the output CSV file.
        output_suffix (str, optional): The suffix to append to the output file name.
            Defaults to "parsed".

    Returns:
        Path: The path to the generated CSV file.

    Raises:
        ExtractionError: If any error occurs during the extraction
            or parsing process.
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
            ), f"Empty DataFrame from _get_dataframe() for {zip_file_path=}"

            # Add static metadata columns
            file_handle.seek(0)
            df["meta_sha1_hash"] = hashlib.sha1(file_handle.read()).hexdigest()
            df["meta_filename"] = df.apply(
                lambda row: f"{zip_file_path}:{row['json_filename']}", axis=1
            )
            df["meta_ingested_at"] = pd.Timestamp.now(tz="UTC").round("ms")

            # Remove the temporary json_filename column
            df = df.drop(columns=["json_filename"])

        # Create the output file path
        base_name = zip_file_path.stem
        output_file_name = f"{base_name}__{output_suffix}.{extension}"
        output_path = output_root / output_file_name

        df.to_csv(output_path, index=False)
        log.debug("Parsed csv with data types:\n%s", df.dtypes)
        log.info("Saved parsed csv file to: %s", output_path)
        return output_path

    except Exception as e:
        raise ExtractionError(f"Error during extraction process: {str(e)}") from e


def transform(
    extracted_table_path: Path, output_root: Path, output_suffix: str = "transformed"
) -> Path:
    """
    Transforms the extracted data, mapping paper titles to authors and their
    emails for research purposes.
    Also includes the full body text as a field which may be used to interpolate
    the country name if not found in the title.

    Args:
        extracted_table_path (Path): The path to the extracted CSV file.
        output_root (Path): The root directory for the output transformed file.
        output_suffix (str, optional): The suffix to append to the output file name.
            Defaults to "transformed".

    Returns:
        Path: The path to the transformed CSV file.

    Raises:
        TransformationError: If any error occurs during the transformation process.
    """
    try:
        assert extracted_table_path.stem.endswith(
            "__parsed"
        ), f"Input file {extracted_table_path} must end with '__parsed'"

        df = pd.read_csv(extracted_table_path)
        log.debug(f"Starting transformation of {extracted_table_path}")
        log.debug(f"Initial DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")

        # Parse metadata and create derived fields
        df["metadata"] = df["metadata"].apply(json.loads)
        df["title"] = df["metadata"].apply(lambda x: x["title"])

        # Parse and concatenate body_text
        df["body_text"] = df["body_text"].apply(json.loads)
        df["full_text"] = df["body_text"].apply(
            lambda x: " ".join([section["text"] for section in x])
        )

        # Create authors column
        df["authors"] = df["metadata"].apply(
            lambda x: [
                {"name": f"{a['first']} {a['last']}", "email": a.get("email", "")}
                for a in x["authors"]
            ]
        )

        # Create and explode authors so that each author is now a separate row
        transformed_df = df[
            [
                "paper_id",
                "title",
                "full_text",
                "authors",
                "meta_sha1_hash",
                "meta_filename",
                "meta_ingested_at",
            ]
        ].copy()
        transformed_df = transformed_df.explode("authors")

        # Extract author name and email
        transformed_df["author_name"] = transformed_df["authors"].apply(
            lambda x: x["name"]
        )
        transformed_df["author_email"] = transformed_df["authors"].apply(
            lambda x: x["email"]
        )
        transformed_df = transformed_df.drop(columns=["authors"])

        # Reorder columns
        column_order = [
            "paper_id",
            "author_name",
            "author_email",
            "title",
            "full_text",
            "meta_sha1_hash",
            "meta_filename",
            "meta_ingested_at",
        ]
        transformed_df = transformed_df[column_order]

        log.debug(
            f"Transformed DataFrame shape: {transformed_df.shape}, columns:"
            f" {transformed_df.columns.tolist()}"
        )

        # Count and log paper_id <-> author_email mappings
        valid_email_count = transformed_df["author_email"].notna().sum()
        log.info(
            f"Total number of paper_id <-> author_email mappings: {valid_email_count}"
        )

        # Debug log for detailed mappings (only for papers with non-None emails)
        paper_author_email_details = transformed_df.groupby("paper_id").agg(
            {"author_email": lambda x: [email for email in x if pd.notna(email)]}
        )
        for paper_id, emails in paper_author_email_details.iterrows():
            if emails["author_email"]:
                log.debug(
                    f"Paper ID: {paper_id}, Author Emails: {emails['author_email']}"
                )

        # Save transformed file
        output_file_name = (
            f"{extracted_table_path.stem.replace('__parsed', '')}__{output_suffix}"
            f"{extracted_table_path.suffix}"
        )
        transformed_file_path = output_root / output_file_name
        transformed_df.to_csv(transformed_file_path, index=False)
        log.info(f"Saved transformed file to: {transformed_file_path}")

        return transformed_file_path

    except Exception as e:
        raise TransformationError(
            f"Error during transformation process: {str(e)}"
        ) from e


def load(
    df_file_path: Path, table_name: str, database_file: Optional[str] = None
) -> None:
    """
    Loads the validated CSV file into a SQLite database.

    Args:
        df_file_path (Path): The path to the validated CSV file.
        table_name (str): The name of the table to create and insert data into.
        database_file (Optional[str], optional): The path to the SQLite database file.
            If None, uses in-memory database. Defaults to None.

    Returns:
        None
    """
    df = pd.read_csv(df_file_path)

    if df.empty:
        log.info(f"The file at {df_file_path} is empty. Skipping load operation.")
        return None

    with sqlite3.connect(database_file if database_file else ":memory:") as conn:
        df.to_sql(table_name, conn, index=False, if_exists="replace")

    log.info(
        f"Data loaded into table: {table_name} -- "
        f"Number of rows: {len(df)} -- "
        f"Columns: {', '.join(df.columns)} -- "
        f"Database: {'In-memory' if not database_file else f'File: {database_file}'}"
    )

    return None


def load_cord19_files(filename: str, database_file: Optional[str] = None) -> str:
    """
    Processes CORD-19 data from a zip file and loads it into a SQLite database.

    This function extracts, transforms, and loads the data from the input zip file.

    Args:
        filename (str): The name of the input zip file.
        database_file (Optional[str], optional): The path to the SQLite database file.
            If None, uses in-memory database. Defaults to None.

    Returns:
        str: The name of the transformed table in the SQLite database.
    """
    file_path, output_root = Path(filename), Path(__file__).parent

    # 1. Extract and parse the data into known schema
    extracted_table_path = extract(zip_file_path=file_path, output_root=output_root)

    # 2. Transform the data for the Research Group
    transformed_file_path = transform(
        extracted_table_path=extracted_table_path, output_root=output_root
    )

    # 3. Load the transformed dataset into SQLite database
    load(
        df_file_path=transformed_file_path,
        table_name=(transformed_table := "cord19__transformed"),
        database_file=database_file,
    )

    return transformed_table


if __name__ == "__main__":
    # Set database_file to None to use an in-memory database or "db.sqlite"
    out_table = load_cord19_files(filename="cord19_mini.zip", database_file=None)

    log.info(f"Transformed table: {out_table}")
