#!/usr/bin/env python3
"""
GCS to S3 to Iceberg ETL Pipeline using DuckDB.

This module implements a data pipeline that:
1. Copies compressed CSV files from Google Cloud Storage to Amazon S3
2. Reads and transforms the data using DuckDB
3. Writes the processed data to Iceberg tables (via Parquet with partitioning)

Replaces the original PySpark implementation with DuckDB for improved performance
and simpler deployment.
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any

import boto3
import duckdb
import pandera as pa
import typer
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from google.cloud import storage
from loguru import logger
from pandera import Column, DataFrameSchema
from pandera.typing import Series

# ============================================================================
# Configuration
# ============================================================================

GCS_BUCKET_NAME = "gd-adjust-export"
S3_BUCKET_NAME = "com.gd-mt-prod.dp.adjust-events"
S3_FOLDER_PATH = "raw-data"
S3_OUTPUT_PATH = "processed-data"
ICEBERG_DATABASE = "celerdata"

PREFIXES = [
    "1ietrnrp8xq8", "ko69zatkf75s", "o6kt928c8bgg", "6gof07xh2cg0", "quvh143djuv4",
    "kyb5zuk18lxc", "3krzf2i9ugcg", "dks139yyt81s", "uqgofx50smbk", "17ohic5adt6o",
    "v8ny1jplczk0", "9nnprfr0ru9s", "ezkr0rrexnnk", "bpc1habv43k0", "ba5wkxr5c3y8",
    "frrjgz5cmn7k", "2auzkzpinhmo", "3615l36hx88w", "31i6js1ykx6o", "2oozhrdqq0sg",
    "lq3hbt9dysjk", "uiz394n7whkw", "7kjguvz34etc", "j407gmj41m2o", "ehop1niki9ds",
    "t5c1326g385c", "ctldkbc1ewow", "iq7ji5gz4cn4", "yos4ycqb1ips", "tza6nsp2eh34",
    "zshz8hwfwagw", "ew56peqqmghs", "fpfac2ufwyyo", "dq3490mutog0", "jkxyp60o5tds",
    "xoorjgohuku8", "4m641rv59xq8", "eaaekboxygw0", "1twg6kd5ccjk", "ui0hcrq7ak8w",
    "db2iez4uvfgg", "rzjo5mmzpedc", "2l9ocdgcupds", "u9c4z8ltrzls", "r6e6xi7w6sxs",
    "2tzyq9oq9d34", "3yospcw98jnk", "pkxieogxhc", "irj1si3db4e8", "74y98wgl3280",
    "a7hplqyubvuo", "9g1tja4t2wow", "69lh5l6dglc0", "flk4rqw5ybr4", "vy50x1q3l1xc",
    "yvv3d00b1gqo", "sycn8u6vtou8", "efb6zp4onmyo", "mis39iepw3r4", "s14pwu95cs1s",
    "4m35woxnabr4", "9s5u7rnv6gow", "5luttqfnk70g", "l3jrdn9mwmww", "lr7xookayzuo",
    "zffb8sghwni8", "uicmdljz4uf4", "x4fwajlghkw0", "we2fbljzi96o", "vefa0uvli4g0",
    "d866q9pg1tz4", "uxzzo3rvnzeo", "tjjyso19v7r4", "h60r713slcsg", "7hht2vonovi8",
    "ujae9uecdips", "tokqdg7d3oxs", "rnlvds4mi874", "j61fv3fzop34", "5d5vwkul1im8",
    "alq1dzjyknwg", "o634irjqgv7k", "l0ito5gsrn5s", "db5chufl296o", "y2ybrph3py4g",
    "72nu3o9732ps", "b4i5oty56olc", "p6vlx2ya2wao", "6bwhomiz5qww", "cn6fdc7fptds",
    "3ikonv7ps3wg", "hxaudii9fhmo", "i0scjed9cfsw", "xp43496auk8w", "237rl7lttrpc",
    "l5ygup2vijgg", "733bxse8g6m8", "qjebuzo1pqm8", "71js1sysl3ls", "ohgah696xekg",
    "87j78uijys8w", "stiq7y97clc0", "k38ounufgtfk",
]

# Event type conditions: (table_suffix, field_to_filter_by, filter_value, allowed_prefixes)
# None means table will be produced for all prefixes
EVENT_CONDITIONS: list[tuple[str, str, str, list[str] | None]] = [
    ("session", "activity_kind", "session", None),
    ("install", "activity_kind", "install", None),
    ("ad_impression", "pp_event_name", "ad_impression", None),
    ("iap_purchased", "pp_event_name", "iap_purchased", None),
    ("ab_test_registered", "pp_event_name", "ab_test_registered", None),
    ("asset_balance", "pp_event_name", "Asset_Balance",
     ["ko69zatkf75s", "17ohic5adt6o", "uqgofx50smbk"]),
    ("asset_balance_lower", "pp_event_name", "asset_balance",
     ["ko69zatkf75s", "17ohic5adt6o", "uqgofx50smbk", "7kjguvz34etc",
      "j407gmj41m2o", "k38ounufgtfk", "fpfac2ufwyyo"]),
    ("earn_asset", "pp_event_name", "earn_asset",
     ["ko69zatkf75s", "17ohic5adt6o", "uqgofx50smbk", "7kjguvz34etc", "k38ounufgtfk"]),
    ("spend_asset", "pp_event_name", "spend_asset",
     ["ko69zatkf75s", "17ohic5adt6o", "uqgofx50smbk", "7kjguvz34etc", "k38ounufgtfk"]),
    ("level_started", "pp_event_name", "level_started", None),
    ("level_finished", "pp_event_name", "level_finished", None),
    ("game_started", "pp_event_name", "game_started",
     ["ezkr0rrexnnk", "uiz394n7whkw", "9nnprfr0ru9s", "frrjgz5cmn7k", "jkxyp60o5tds",
      "zshz8hwfwagw", "bpc1habv43k0", "xp43496auk8w", "237rl7lttrpc", "l5ygup2vijgg",
      "stiq7y97clc0"]),
    ("game_finished", "pp_event_name", "game_finished",
     ["ezkr0rrexnnk", "uiz394n7whkw", "9nnprfr0ru9s", "frrjgz5cmn7k", "jkxyp60o5tds",
      "zshz8hwfwagw", "bpc1habv43k0", "xp43496auk8w", "237rl7lttrpc", "l5ygup2vijgg",
      "stiq7y97clc0"]),
    ("prop_placed", "pp_event_name", "prop_placed", ["ehop1niki9ds"]),
    ("navigation_action", "pp_event_name", "navigation_action", ["vefa0uvli4g0"]),
    ("vip_level", "pp_event_name", "VIP_level", ["vefa0uvli4g0"]),
    ("unlockpopup_theme", "pp_event_name", "unlockpopup_theme", ["we2fbljzi96o"]),
    ("unlockpopup_hardmode", "pp_event_name", "unlockpopup_hardmode", ["we2fbljzi96o"]),
    ("arena_fights", "pp_event_name", "arena_fights", ["t5c1326g385c"]),
    ("hero_data", "pp_event_name", "hero_data", ["t5c1326g385c"]),
    ("power_up", "pp_event_name", "power_up", ["t5c1326g385c"]),
    ("ad_call", "pp_event_name", "ad_call",
     ["y2ybrph3py4g", "2auzkzpinhmo", "xoorjgohuku8"]),
    ("vip_bundle", "pp_event_name", "vip_bundle", ["y2ybrph3py4g"]),
    ("skin_bundle", "pp_event_name", "skin_bundle", ["y2ybrph3py4g"]),
    ("ad_failed", "pp_event_name", "ad_failed", ["o634irjqgv7k"]),
    ("menu_items", "pp_event_name", "menu_items", ["o634irjqgv7k"]),
    ("line_attempts", "pp_event_name", "line_attempts", ["2auzkzpinhmo"]),
    ("depth_reached", "pp_event_name", "depth_reached", ["eaaekboxygw0"]),
    ("ores_collected", "pp_event_name", "ores_collected", ["eaaekboxygw0"]),
    ("upgrades", "pp_event_name", "upgrades", ["eaaekboxygw0"]),
    ("achievements", "pp_event_name", "achievements", ["eaaekboxygw0"]),
    ("vehicle_used", "pp_event_name", "vehicle_used", ["ui0hcrq7ak8w"]),
    ("gun_used", "pp_event_name", "gun_used", ["ui0hcrq7ak8w"]),
    ("menu", "pp_event_name", "menu", ["ui0hcrq7ak8w"]),
    ("openworld_exploration", "pp_event_name", "openworld_exploration", ["ui0hcrq7ak8w"]),
    ("infinite_mode_started", "pp_event_name", "infinite_mode_started", ["74y98wgl3280"]),
    ("infinite_mode_finished", "pp_event_name", "infinite_mode_finished", ["74y98wgl3280"]),
    ("mode_started", "pp_event_name", "mode_started", ["efb6zp4onmyo"]),
    ("mode_finished", "pp_event_name", "mode_finished", ["efb6zp4onmyo"]),
    ("drawing_started", "pp_event_name", "drawing_started", ["uicmdljz4uf4"]),
    ("drawing_finish", "pp_event_name", "drawing_finish", ["uicmdljz4uf4"]),
    ("color_start", "pp_event_name", "color_start", ["uicmdljz4uf4"]),
    ("color_finish", "pp_event_name", "color_finish", ["uicmdljz4uf4"]),
    ("tool_started", "pp_event_name", "tool_started", ["h60r713slcsg"]),
    ("tool_finished", "pp_event_name", "tool_finished", ["h60r713slcsg"]),
    ("tutorial_completed", "pp_event_name", "tutorial_completed", ["k38ounufgtfk"]),
    ("day_completed", "pp_event_name", "day_completed", ["k38ounufgtfk"]),
]

REQUIRED_COLUMNS = [
    "adid", "city", "country", "device_name", "device_type", "event_name", "event",
    "lifetime_session_count", "language", "os_name", "os_version", "random_user_id",
    "session_count", "time_spent", "activity_kind", "app_name", "app_version",
    "created_at", "device_manufacturer", "installed_at", "last_session_time",
    "network_name", "app_token", "publisher_parameters", "received_at", "timezone",
    "app_version_short", "last_time_spent", "last_session_length", "campaign_name",
    "creative_name", "match_type", "tracker", "tracker_name", "adgroup_name",
    "first_tracker_name", "first_tracker", "last_tracker_name", "last_tracker",
    "gps_adid", "google_app_set_id", "idfa", "idfv", "dcp_tapjoy_click_id",
    "external_campaign_id", "external_adgroup_id", "external_creative_id", "partner",
]

TIMESTAMP_COLUMNS = ["created_at", "installed_at", "last_session_time", "received_at"]

ALLOWED_ACTIVITY_KINDS = ["session", "install", "install_update", "event"]


# ============================================================================
# Logging Configuration
# ============================================================================

def configure_logging(verbose: bool = False, log_file: Path | None = None) -> None:
    """Configure loguru with appropriate settings."""
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, format=log_format, level=level, colorize=True)

    if log_file:
        logger.add(
            log_file,
            format=log_format,
            level="DEBUG",
            rotation="100 MB",
            retention="7 days",
            compression="gz",
        )


# ============================================================================
# Data Validation Schemas (Pandera)
# ============================================================================

class PublisherParametersSchema(pa.DataFrameModel):
    """Schema for publisher_parameters JSON structure."""

    adjust_event_name: Series[str] = pa.Field(nullable=True)
    game: Series[str] = pa.Field(nullable=True)
    ad_format: Series[str] = pa.Field(nullable=True)
    ad_placement: Series[str] = pa.Field(nullable=True)
    ad_revenue: Series[str] = pa.Field(nullable=True)


class RawEventSchema(pa.DataFrameModel):
    """Schema for raw event data validation."""

    created_at: Series[float] = pa.Field(nullable=False, coerce=True)
    activity_kind: Series[str] = pa.Field(
        nullable=False,
        isin=ALLOWED_ACTIVITY_KINDS,
    )
    adid: Series[str] = pa.Field(nullable=True)
    country: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False


def create_validation_schema() -> DataFrameSchema:
    """Create a Pandera schema for validating raw event data."""
    return DataFrameSchema(
        columns={
            "created_at": Column(nullable=False),
            "activity_kind": Column(
                str,
                checks=pa.Check.isin(ALLOWED_ACTIVITY_KINDS),
                nullable=False,
            ),
        },
        coerce=True,
        strict=False,
    )


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class JobMetrics:
    """Container for job execution metrics."""

    files_copied: int = 0
    rows_read: int = 0
    partitioned_rows_written: int = 0
    aggregate_rows_written: int = 0
    failed_tables: list[str] = field(default_factory=list)
    empty_tables: list[str] = field(default_factory=list)
    phase_timings: dict[str, float] = field(default_factory=dict)

    def log_summary(self) -> None:
        """Log a summary of the job metrics."""
        logger.info("=" * 60)
        logger.info("Job Summary")
        logger.info("=" * 60)
        logger.info(f"Files copied from GCS to S3: {self.files_copied:,}")
        logger.info(f"Rows read from S3: {self.rows_read:,}")
        logger.info(f"Partitioned tables rows written: {self.partitioned_rows_written:,}")
        logger.info(f"Aggregate tables rows written: {self.aggregate_rows_written:,}")

        if self.failed_tables:
            logger.warning(f"Failed tables ({len(self.failed_tables)}): {self.failed_tables}")

        if self.empty_tables:
            logger.info(f"Empty tables ({len(self.empty_tables)}): {self.empty_tables}")

        logger.info("Phase timings:")
        for phase, duration in self.phase_timings.items():
            logger.info(f"  {phase}: {duration:.2f}s")


@dataclass
class ETLConfig:
    """Configuration for the ETL pipeline."""

    date: str
    hour: str | None
    gcs_bucket: str = GCS_BUCKET_NAME
    s3_bucket: str = S3_BUCKET_NAME
    s3_folder_path: str = S3_FOLDER_PATH
    s3_output_path: str = S3_OUTPUT_PATH
    iceberg_database: str = ICEBERG_DATABASE
    prefixes: list[str] = field(default_factory=lambda: PREFIXES.copy())
    max_workers: int = 32
    dry_run: bool = False
    gcs_credentials_path: str = "gcs_key.json"


# ============================================================================
# GCS to S3 Copy Functions
# ============================================================================

class GCSToS3Copier:
    """Handles copying files from GCS to S3."""

    def __init__(self, config: ETLConfig, gcs_credentials: dict[str, Any] | None = None):
        self.config = config
        self.gcs_credentials = gcs_credentials
        self._init_clients()

    def _init_clients(self) -> None:
        """Initialize GCS and S3 clients."""
        if self.gcs_credentials:
            self.gcs_client = storage.Client.from_service_account_info(self.gcs_credentials)
        else:
            self.gcs_client = storage.Client()

        self.gcs_bucket = self.gcs_client.bucket(self.config.gcs_bucket)

        self.s3_client = boto3.client(
            "s3",
            config=BotoConfig(
                max_pool_connections=50,
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )

    def copy_prefix(self, prefix: str) -> list[str]:
        """Copy all matching files for a given prefix from GCS to S3."""
        copied_files = []

        if self.config.hour:
            gcs_prefix_path = f"{prefix}_{self.config.date}T{self.config.hour}"
        else:
            gcs_prefix_path = f"{prefix}_{self.config.date}T"

        try:
            blobs = list(self.gcs_bucket.list_blobs(prefix=gcs_prefix_path))
            csv_gz_blobs = [b for b in blobs if b.name.endswith(".csv.gz")]

            for blob in csv_gz_blobs:
                s3_key = f"{self.config.s3_folder_path}/{self.config.date}/{prefix}/{blob.name}"

                try:
                    if self.config.dry_run:
                        logger.debug(f"[DRY-RUN] Would copy {blob.name} to s3://{self.config.s3_bucket}/{s3_key}")
                        copied_files.append(s3_key)
                        continue

                    # Use streaming for large files (>5MB)
                    if blob.size and blob.size > 5 * 1024 * 1024:
                        buffer = BytesIO()
                        blob.download_to_file(buffer, raw_download=True)
                        buffer.seek(0)
                        self.s3_client.upload_fileobj(buffer, self.config.s3_bucket, s3_key)
                        buffer.close()
                    else:
                        gcs_data = blob.download_as_bytes(raw_download=True)
                        self.s3_client.put_object(
                            Bucket=self.config.s3_bucket,
                            Key=s3_key,
                            Body=gcs_data,
                        )

                    copied_files.append(s3_key)
                    logger.debug(f"Copied {blob.name} to s3://{self.config.s3_bucket}/{s3_key}")

                except ClientError as e:
                    logger.error(f"Error uploading {blob.name} to S3: {e}")

            if csv_gz_blobs:
                logger.info(f"Copied {len(csv_gz_blobs)} files for prefix {prefix}")

        except Exception as e:
            logger.error(f"Error processing prefix {prefix}: {e}")

        return copied_files

    def copy_all_prefixes(self) -> list[str]:
        """Copy files for all prefixes using parallel execution."""
        all_copied_files: list[str] = []
        max_workers = min(self.config.max_workers, len(self.config.prefixes))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_prefix = {
                executor.submit(self.copy_prefix, prefix): prefix
                for prefix in self.config.prefixes
            }

            for future in as_completed(future_to_prefix):
                prefix = future_to_prefix[future]
                try:
                    copied = future.result()
                    all_copied_files.extend(copied)
                except Exception as e:
                    logger.error(f"Exception processing prefix {prefix}: {e}")

        return all_copied_files


# ============================================================================
# S3 Path Resolution
# ============================================================================

class S3PathResolver:
    """Resolves and validates S3 paths for data files."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.s3_client = boto3.client("s3")

    def resolve_paths_for_prefix(self, prefix: str) -> tuple[list[str], str | None]:
        """Resolve S3 paths for a single prefix."""
        if self.config.hour:
            file_prefix_path = f"{prefix}_{self.config.date}T{self.config.hour}"
        else:
            file_prefix_path = f"{prefix}_{self.config.date}T"

        search_prefix = (
            f"{self.config.s3_folder_path}/{self.config.date}/{prefix}/{file_prefix_path}"
        )

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.s3_bucket,
                Prefix=search_prefix,
            )

            matched_paths = []
            if "Contents" in response:
                matched_paths = [
                    f"s3://{self.config.s3_bucket}/{obj['Key']}"
                    for obj in response["Contents"]
                    if obj["Key"].endswith(".csv.gz")
                ]

            if matched_paths:
                return matched_paths, None
            return [], f"s3://{self.config.s3_bucket}/{search_prefix}*.csv.gz"

        except ClientError as e:
            logger.error(f"Error listing S3 objects for prefix {prefix}: {e}")
            return [], f"s3://{self.config.s3_bucket}/{search_prefix}*.csv.gz"

    def resolve_all_paths(self) -> tuple[list[str], list[str]]:
        """Resolve S3 paths for all prefixes using parallel execution."""
        existing_paths: list[str] = []
        missing_patterns: list[str] = []
        max_workers = min(self.config.max_workers, len(self.config.prefixes))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_prefix = {
                executor.submit(self.resolve_paths_for_prefix, prefix): prefix
                for prefix in self.config.prefixes
            }

            for future in as_completed(future_to_prefix):
                prefix = future_to_prefix[future]
                try:
                    paths, missing = future.result()
                    existing_paths.extend(paths)
                    if missing:
                        missing_patterns.append(missing)
                except Exception as e:
                    logger.error(f"Exception resolving paths for prefix {prefix}: {e}")

        return existing_paths, missing_patterns


# ============================================================================
# DuckDB Data Processor
# ============================================================================

class DuckDBProcessor:
    """Handles data processing using DuckDB."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.conn = duckdb.connect(":memory:")
        self._setup_extensions()
        self._setup_s3_credentials()

    def _setup_extensions(self) -> None:
        """Install and load required DuckDB extensions."""
        extensions = ["httpfs", "aws", "parquet", "json"]
        for ext in extensions:
            self.conn.execute(f"INSTALL {ext}")
            self.conn.execute(f"LOAD {ext}")
        logger.debug("DuckDB extensions loaded")

    def _setup_s3_credentials(self) -> None:
        """Configure S3 credentials using AWS credential chain."""
        self.conn.execute("""
            CREATE SECRET IF NOT EXISTS s3_secret (
                TYPE s3,
                PROVIDER credential_chain
            )
        """)
        logger.debug("S3 credentials configured")

    def load_csv_files(self, s3_paths: list[str]) -> int:
        """Load CSV files from S3 into a DuckDB table."""
        if not s3_paths:
            logger.warning("No S3 paths provided for loading")
            return 0

        # Create a list of paths for DuckDB
        paths_str = ", ".join(f"'{p}'" for p in s3_paths)

        # Read CSV files with auto-detection
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE raw_events AS
            SELECT
                *,
                regexp_extract(filename, '([^/]+)_\\d{{4}}-\\d{{2}}-\\d{{2}}T', 1) AS prefix
            FROM read_csv(
                [{paths_str}],
                header = true,
                quote = '"',
                escape = '"',
                filename = true,
                union_by_name = true,
                ignore_errors = true
            )
        """)

        row_count = self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
        logger.info(f"Loaded {row_count:,} rows from {len(s3_paths)} files")
        return row_count

    def process_dataframe(self, current_time: str) -> None:
        """Apply transformations to the raw data."""
        # Normalize column names (strip, remove braces, lowercase)
        columns = self.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_events'"
        ).fetchall()

        rename_clauses = []
        for (col,) in columns:
            clean_col = col.strip().replace("{", "").replace("}", "").lower()
            if clean_col != col:
                rename_clauses.append(f'"{col}" AS "{clean_col}"')
            else:
                rename_clauses.append(f'"{col}"')

        # Create normalized table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE normalized_events AS
            SELECT {', '.join(rename_clauses)}
            FROM raw_events
        """)

        # Parse publisher_parameters JSON and apply filters
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE processed_events AS
            SELECT
                *,
                COALESCE(
                    json_extract_string(publisher_parameters, '$.adjust_event_name'),
                    NULL
                ) AS pp_event_name,
                COALESCE(
                    json_extract_string(publisher_parameters, '$.game'),
                    NULL
                ) AS pp_game,
                COALESCE(
                    json_extract_string(publisher_parameters, '$.ad_format'),
                    NULL
                ) AS pp_ad_format,
                COALESCE(
                    json_extract_string(publisher_parameters, '$.ad_placement'),
                    NULL
                ) AS pp_ad_placement,
                COALESCE(
                    json_extract_string(publisher_parameters, '$.ad_revenue'),
                    NULL
                ) AS pp_ad_revenue,
                '{current_time}' AS _time,
                -- Convert Unix timestamp to date
                CASE
                    WHEN CAST(created_at AS BIGINT) >= 1000000000000
                    THEN DATE(to_timestamp(CAST(created_at AS BIGINT) / 1000))
                    ELSE DATE(to_timestamp(CAST(created_at AS BIGINT)))
                END AS date
            FROM normalized_events
            WHERE created_at IS NOT NULL
              AND LOWER(TRIM(CAST(activity_kind AS VARCHAR))) IN ('session', 'install', 'install_update', 'event')
        """)

        row_count = self.conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]
        logger.info(f"Processed {row_count:,} rows after filtering")

    def get_distinct_prefixes(self) -> list[str]:
        """Get distinct prefixes from the processed data."""
        result = self.conn.execute(
            "SELECT DISTINCT prefix FROM processed_events WHERE prefix IS NOT NULL"
        ).fetchall()
        return [row[0] for row in result]

    def write_partitioned_table(
        self,
        prefix: str,
        table_suffix: str,
        filter_column: str,
        filter_value: str,
    ) -> int:
        """Write filtered data to S3 as partitioned Parquet."""
        table_name = f"{prefix}_{table_suffix.lower()}"
        output_path = f"s3://{self.config.s3_bucket}/{self.config.s3_output_path}/{self.config.iceberg_database}/{table_name}"

        # Build column selection (exclude internal columns)
        exclude_cols = {"prefix", "pp_game", "pp_ad_format", "pp_ad_placement", "pp_ad_revenue"}
        columns = self.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'processed_events'"
        ).fetchall()
        select_cols = [f'"{col[0]}"' for col in columns if col[0] not in exclude_cols]

        # Count matching rows
        count_result = self.conn.execute(f"""
            SELECT COUNT(*) FROM processed_events
            WHERE prefix = '{prefix}' AND "{filter_column}" = '{filter_value}'
        """).fetchone()[0]

        if count_result == 0:
            return 0

        # Write to Parquet with partitioning
        self.conn.execute(f"""
            COPY (
                SELECT {', '.join(select_cols)}
                FROM processed_events
                WHERE prefix = '{prefix}' AND "{filter_column}" = '{filter_value}'
            )
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd')
        """)

        logger.info(f"Wrote {count_result:,} rows to {table_name}")
        return count_result

    def write_ad_impression_aggregates(
        self,
        current_time: str,
    ) -> tuple[int, list[str]]:
        """Compute and write ad impression aggregates."""
        # Filter for ad impressions in last 30 days
        count = self.conn.execute("""
            SELECT COUNT(*) FROM processed_events
            WHERE pp_event_name = 'ad_impression'
              AND date >= CURRENT_DATE - INTERVAL 30 DAY
        """).fetchone()[0]

        if count == 0:
            logger.info("No ad impression rows found in the last 30 days")
            return 0, []

        # Create aggregates table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE ad_aggregates AS
            SELECT
                prefix,
                date,
                adid,
                country,
                os_name,
                device_type,
                app_version_short,
                pp_game AS game_name,
                pp_ad_format AS ad_format,
                pp_ad_placement AS ad_placement,
                COUNT(*) AS ad_count,
                SUM(CAST(pp_ad_revenue AS DOUBLE)) AS ad_revenue,
                MAX(
                    DATE_DIFF('day',
                        CASE
                            WHEN CAST(installed_at AS BIGINT) >= 1000000000000
                            THEN to_timestamp(CAST(installed_at AS BIGINT) / 1000)
                            ELSE to_timestamp(CAST(installed_at AS BIGINT))
                        END,
                        CASE
                            WHEN CAST(created_at AS BIGINT) >= 1000000000000
                            THEN to_timestamp(CAST(created_at AS BIGINT) / 1000)
                            ELSE to_timestamp(CAST(created_at AS BIGINT))
                        END
                    )
                ) AS aging,
                '{current_time}' AS _time
            FROM processed_events
            WHERE pp_event_name = 'ad_impression'
              AND date >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY
                prefix, date, adid, country, os_name, device_type,
                app_version_short, pp_game, pp_ad_format, pp_ad_placement
        """)

        # Get distinct prefixes for aggregates
        prefixes = self.conn.execute(
            "SELECT DISTINCT prefix FROM ad_aggregates WHERE prefix IS NOT NULL"
        ).fetchall()

        total_rows = 0
        failed_tables: list[str] = []

        for (prefix,) in prefixes:
            table_name = f"{prefix}_ad_impression_revenue_preaggregates"
            output_path = f"s3://{self.config.s3_bucket}/{self.config.s3_output_path}/{self.config.iceberg_database}/{table_name}"

            try:
                row_count = self.conn.execute(f"""
                    SELECT COUNT(*) FROM ad_aggregates WHERE prefix = '{prefix}'
                """).fetchone()[0]

                if row_count == 0:
                    continue

                self.conn.execute(f"""
                    COPY (
                        SELECT
                            date,
                            adid AS user_id,
                            country,
                            os_name,
                            device_type,
                            app_version_short,
                            game_name,
                            ad_format,
                            ad_placement,
                            NULL AS iap_product_id,
                            ad_count,
                            ad_revenue,
                            0 AS iap_count,
                            0.0 AS iap_revenue,
                            0 AS session_count,
                            aging,
                            _time
                        FROM ad_aggregates
                        WHERE prefix = '{prefix}'
                    )
                    TO '{output_path}'
                    (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd')
                """)

                total_rows += row_count
                logger.info(f"Wrote {row_count:,} rows to {table_name}")

            except Exception as e:
                logger.error(f"Failed to write table {table_name}: {e}")
                failed_tables.append(table_name)

        return total_rows, failed_tables

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()


# ============================================================================
# Main ETL Pipeline
# ============================================================================

class ETLPipeline:
    """Orchestrates the complete ETL pipeline."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.metrics = JobMetrics()
        self.gcs_credentials = self._load_gcs_credentials()

    def _load_gcs_credentials(self) -> dict[str, Any] | None:
        """Load GCS service account credentials if available."""
        if os.path.exists(self.config.gcs_credentials_path):
            with open(self.config.gcs_credentials_path, encoding="utf-8") as f:
                return json.load(f)
        return None

    def run(self) -> JobMetrics:
        """Execute the complete ETL pipeline."""
        logger.info("Starting GCS to S3 to Iceberg ETL Pipeline")
        logger.info(f"Date: {self.config.date}, Hour: {self.config.hour or 'all'}")
        logger.info(f"Processing {len(self.config.prefixes)} prefixes")

        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Phase 1: Copy from GCS to S3
            self._run_phase1_copy()

            # Phase 2: Resolve S3 paths
            s3_paths = self._run_phase2_resolve_paths()
            if not s3_paths:
                logger.warning("No source files found. Exiting.")
                return self.metrics

            # Phase 3: Load data into DuckDB
            processor = self._run_phase3_load_data(s3_paths)

            # Phase 4: Process/transform data
            self._run_phase4_process_data(processor, current_time)

            # Phase 5: Write partitioned tables
            self._run_phase5_write_partitioned(processor)

            # Phase 6: Write aggregates
            self._run_phase6_write_aggregates(processor, current_time)

            processor.close()

        except Exception as e:
            logger.exception(f"Pipeline failed with error: {e}")
            raise

        self.metrics.log_summary()
        return self.metrics

    def _run_phase1_copy(self) -> None:
        """Phase 1: Copy files from GCS to S3."""
        logger.info("PHASE 1: Copying files from GCS to S3")
        start = time.perf_counter()

        copier = GCSToS3Copier(self.config, self.gcs_credentials)
        copied_files = copier.copy_all_prefixes()
        self.metrics.files_copied = len(copied_files)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 1 - GCS to S3 Copy"] = duration
        logger.info(f"PHASE 1 completed: {len(copied_files)} files in {duration:.2f}s")

    def _run_phase2_resolve_paths(self) -> list[str]:
        """Phase 2: Resolve S3 paths."""
        logger.info("PHASE 2: Resolving S3 paths")
        start = time.perf_counter()

        resolver = S3PathResolver(self.config)
        s3_paths, missing = resolver.resolve_all_paths()

        for pattern in missing:
            logger.debug(f"No files found for pattern: {pattern}")

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 2 - S3 Path Resolution"] = duration
        logger.info(f"PHASE 2 completed: {len(s3_paths)} paths in {duration:.2f}s")

        return s3_paths

    def _run_phase3_load_data(self, s3_paths: list[str]) -> DuckDBProcessor:
        """Phase 3: Load data into DuckDB."""
        logger.info("PHASE 3: Loading data into DuckDB")
        start = time.perf_counter()

        processor = DuckDBProcessor(self.config)
        self.metrics.rows_read = processor.load_csv_files(s3_paths)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 3 - Data Loading"] = duration
        logger.info(f"PHASE 3 completed: {self.metrics.rows_read:,} rows in {duration:.2f}s")

        return processor

    def _run_phase4_process_data(self, processor: DuckDBProcessor, current_time: str) -> None:
        """Phase 4: Process and transform data."""
        logger.info("PHASE 4: Processing data")
        start = time.perf_counter()

        processor.process_dataframe(current_time)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 4 - Data Processing"] = duration
        logger.info(f"PHASE 4 completed in {duration:.2f}s")

    def _run_phase5_write_partitioned(self, processor: DuckDBProcessor) -> None:
        """Phase 5: Write partitioned tables."""
        logger.info("PHASE 5: Writing partitioned tables")
        start = time.perf_counter()

        prefixes = processor.get_distinct_prefixes()

        for prefix in prefixes:
            for table_suffix, filter_col, filter_val, allowed_prefixes in EVENT_CONDITIONS:
                # Skip if this event type is not allowed for this prefix
                if allowed_prefixes is not None and prefix not in allowed_prefixes:
                    continue

                table_name = f"{prefix}_{table_suffix.lower()}"

                try:
                    rows = processor.write_partitioned_table(
                        prefix, table_suffix, filter_col, filter_val
                    )
                    if rows > 0:
                        self.metrics.partitioned_rows_written += rows
                    else:
                        self.metrics.empty_tables.append(table_name)
                except Exception as e:
                    logger.error(f"Failed to write table {table_name}: {e}")
                    self.metrics.failed_tables.append(table_name)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 5 - Partitioned Tables"] = duration
        logger.info(
            f"PHASE 5 completed: {self.metrics.partitioned_rows_written:,} rows in {duration:.2f}s"
        )

    def _run_phase6_write_aggregates(
        self, processor: DuckDBProcessor, current_time: str
    ) -> None:
        """Phase 6: Write aggregate tables."""
        logger.info("PHASE 6: Writing aggregate tables")
        start = time.perf_counter()

        rows, failed = processor.write_ad_impression_aggregates(current_time)
        self.metrics.aggregate_rows_written = rows
        self.metrics.failed_tables.extend(failed)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 6 - Aggregates"] = duration
        logger.info(f"PHASE 6 completed: {rows:,} rows in {duration:.2f}s")


# ============================================================================
# CLI Application
# ============================================================================

app = typer.Typer(
    name="gcs-to-s3-etl",
    help="ETL Pipeline: GCS → S3 → Iceberg (DuckDB)",
    add_completion=False,
)


@app.command()
def run(
    date: Annotated[
        str,
        typer.Option(
            "--date", "-d",
            help="Date to process (YYYY-MM-DD format)",
        ),
    ],
    hour: Annotated[
        str | None,
        typer.Option(
            "--hour", "-h",
            help="Hour to process (HHMM format). If not provided, processes all hours.",
        ),
    ] = None,
    prefixes: Annotated[
        str | None,
        typer.Option(
            "--prefixes", "-p",
            help="Comma-separated list of prefixes to process. Defaults to all.",
        ),
    ] = None,
    gcs_bucket: Annotated[
        str,
        typer.Option(
            "--gcs-bucket",
            help="GCS source bucket name",
        ),
    ] = GCS_BUCKET_NAME,
    s3_bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="S3 destination bucket name",
        ),
    ] = S3_BUCKET_NAME,
    max_workers: Annotated[
        int,
        typer.Option(
            "--max-workers", "-w",
            help="Maximum number of parallel workers",
        ),
    ] = 32,
    gcs_credentials: Annotated[
        str,
        typer.Option(
            "--gcs-credentials",
            help="Path to GCS service account JSON file",
        ),
    ] = "gcs_key.json",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v",
            help="Enable verbose logging",
        ),
    ] = False,
    log_file: Annotated[
        Path | None,
        typer.Option(
            "--log-file",
            help="Path to log file",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Simulate operations without making changes",
        ),
    ] = False,
) -> None:
    """
    Run the GCS to S3 to Iceberg ETL pipeline.

    This pipeline copies data from Google Cloud Storage to Amazon S3,
    transforms it using DuckDB, and writes it as partitioned Parquet files.
    """
    configure_logging(verbose=verbose, log_file=log_file)

    # Parse prefixes if provided
    prefix_list = PREFIXES.copy()
    if prefixes:
        prefix_list = [p.strip() for p in prefixes.split(",")]

    config = ETLConfig(
        date=date,
        hour=hour,
        gcs_bucket=gcs_bucket,
        s3_bucket=s3_bucket,
        prefixes=prefix_list,
        max_workers=max_workers,
        dry_run=dry_run,
        gcs_credentials_path=gcs_credentials,
    )

    pipeline = ETLPipeline(config)
    metrics = pipeline.run()

    # Exit with error code if there were failures
    if metrics.failed_tables:
        raise typer.Exit(code=1)


@app.command()
def list_prefixes() -> None:
    """List all configured prefixes."""
    typer.echo("Configured prefixes:")
    for i, prefix in enumerate(PREFIXES, 1):
        typer.echo(f"  {i:3d}. {prefix}")
    typer.echo(f"\nTotal: {len(PREFIXES)} prefixes")


@app.command()
def list_events() -> None:
    """List all configured event types."""
    typer.echo("Configured event types:")
    typer.echo("-" * 80)
    for table_suffix, filter_col, filter_val, allowed_prefixes in EVENT_CONDITIONS:
        prefix_info = "all" if allowed_prefixes is None else f"{len(allowed_prefixes)} prefixes"
        typer.echo(f"  {table_suffix:30s} | {filter_col}={filter_val:20s} | {prefix_info}")
    typer.echo(f"\nTotal: {len(EVENT_CONDITIONS)} event types")


@app.command()
def validate_config() -> None:
    """Validate the configuration and check connectivity."""
    configure_logging(verbose=True)

    logger.info("Validating configuration...")

    # Check GCS credentials
    gcs_creds_path = Path("gcs_key.json")
    if gcs_creds_path.exists():
        logger.info("✓ GCS credentials file found")
    else:
        logger.warning("✗ GCS credentials file not found (will use default credentials)")

    # Check S3 connectivity
    try:
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=S3_BUCKET_NAME)
        logger.info(f"✓ S3 bucket '{S3_BUCKET_NAME}' is accessible")
    except Exception as e:
        logger.error(f"✗ S3 bucket check failed: {e}")

    # Check DuckDB
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("SELECT 1")
        conn.close()
        logger.info("✓ DuckDB is working")
    except Exception as e:
        logger.error(f"✗ DuckDB check failed: {e}")

    logger.info("Configuration validation complete")


def main() -> None:
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
