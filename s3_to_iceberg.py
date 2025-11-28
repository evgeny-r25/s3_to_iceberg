#!/usr/bin/env python3
"""
S3 to Iceberg ETL Pipeline using DuckDB.

This module implements a data pipeline that:
1. Discovers and reads compressed CSV files from S3 source bucket
2. Transforms the data using DuckDB
3. Writes the processed data to Iceberg tables (via Parquet with partitioning)

Uses DuckDB to directly read from S3, eliminating the need for GCS or intermediate storage.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import boto3
import duckdb
import typer
from loguru import logger

# ============================================================================
# Configuration
# ============================================================================

S3_SOURCE_BUCKET = "dev-raw-data-testing"
S3_SOURCE_PREFIX = "dev-source-raw"
S3_BUCKET_NAME = "dev-raw-data-testing"
S3_OUTPUT_PATH = "dev-iceberg-warehouse"
ICEBERG_DATABASE = "default"

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

    s3_source_bucket: str = S3_SOURCE_BUCKET
    s3_source_prefix: str = S3_SOURCE_PREFIX
    s3_bucket: str = S3_BUCKET_NAME
    s3_output_path: str = S3_OUTPUT_PATH
    iceberg_database: str = ICEBERG_DATABASE
    prefixes: list[str] = field(default_factory=lambda: PREFIXES.copy())
    max_workers: int = 32
    dry_run: bool = False


# ============================================================================
# S3 File Discovery Functions
# ============================================================================

class S3FileDiscovery:
    """Discovers compressed CSV files in S3 using boto3."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.s3_client = boto3.client('s3')

    def discover_files(self) -> list[str]:
        """Discover all CSV.gz files in S3 source bucket using boto3 list_objects."""
        try:
            logger.debug(f"Discovering files in s3://{self.config.s3_source_bucket}/{self.config.s3_source_prefix}")

            # List all objects in the prefix
            s3_files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.config.s3_source_bucket,
                Prefix=self.config.s3_source_prefix
            )

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.csv.gz'):
                            s3_path = f"s3://{self.config.s3_source_bucket}/{key}"
                            s3_files.append(s3_path)

            logger.info(f"Discovered {len(s3_files)} CSV.gz files in s3://{self.config.s3_source_bucket}/{self.config.s3_source_prefix}")

            if s3_files:
                for f in s3_files[:5]:  # Log first 5 files
                    logger.debug(f"Found file: {f}")

            return s3_files

        except Exception as e:
            logger.error(f"Error discovering files: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return []

    def close(self) -> None:
        """Close resources (no-op for boto3)."""
        pass


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

        # Set S3 region
        self.conn.execute("SET s3_region = 'eu-west-1'")
        logger.debug("DuckDB extensions loaded")

    def _setup_s3_credentials(self) -> None:
        """Configure S3 credentials using AWS environment variables or credentials."""
        try:
            # Try to use AWS credential chain first
            self.conn.execute("""
                CREATE SECRET IF NOT EXISTS s3_secret (
                    TYPE s3,
                    PROVIDER credential_chain
                )
            """)
            logger.debug("S3 credentials configured using credential chain")
        except Exception as e:
            logger.debug(f"Credential chain failed: {e}, trying environment variables")
            try:
                # Fall back to environment variables
                import os
                access_key = os.getenv('AWS_ACCESS_KEY_ID')
                secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

                if access_key and secret_key:
                    self.conn.execute(f"""
                        CREATE SECRET IF NOT EXISTS s3_secret (
                            TYPE s3,
                            KEY_ID '{access_key}',
                            SECRET '{secret_key}'
                        )
                    """)
                    logger.debug("S3 credentials configured using environment variables")
                else:
                    logger.warning("AWS credentials not found in environment variables")
            except Exception as e2:
                logger.error(f"Failed to configure S3 credentials: {e2}")

    def load_csv_files(self, s3_paths: list[str]) -> int:
        """Load CSV files from S3 into a DuckDB table, moving failed files to DLQ."""
        if not s3_paths:
            logger.warning("No S3 paths provided for loading")
            return 0

        s3_client = boto3.client('s3', region_name='eu-west-1')
        successful_paths = []
        failed_files = []

        # Process each file individually to catch errors per file
        for s3_path in s3_paths:
            try:
                logger.debug(f"Attempting to read: {s3_path}")

                # Parse S3 path
                parts = s3_path.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key = parts[1]

                # Try to read CSV.gz file with DuckDB
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE temp_csv AS
                    SELECT
                        *,
                        regexp_extract(filename, '([^/]+)_\\d{{4}}-\\d{{2}}-\\d{{2}}T', 1) AS prefix
                    FROM read_csv(
                        '{s3_path}',
                        header = true,
                        quote = '"',
                        escape = '"',
                        filename = true,
                        union_by_name = true,
                        ignore_errors = true
                    )
                """)

                row_count = self.conn.execute("SELECT COUNT(*) FROM temp_csv").fetchone()[0]
                logger.info(f"Successfully loaded {row_count:,} rows from {s3_path}")
                successful_paths.append(s3_path)

            except Exception as e:
                logger.error(f"Failed to read {s3_path}: {e}")
                failed_files.append((s3_path, str(e)))

                # Move failed file to DLQ
                try:
                    self._move_file_to_dlq(s3_path, str(e))
                except Exception as dlq_error:
                    logger.error(f"Failed to move {s3_path} to DLQ: {dlq_error}")

        # Create final table from successful files
        if successful_paths:
            paths_str = ", ".join(f"'{p}'" for p in successful_paths)

            # Create raw table first
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_events_temp AS
                SELECT
                    *
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

            # Get all column names and create clean versions
            columns = self.conn.execute("DESCRIBE raw_events_temp").fetchall()
            select_clauses = []
            for col_name, _ in columns:
                # Remove curly braces from column names if present
                clean_name = col_name.replace('{', '').replace('}', '')
                if col_name != clean_name:
                    select_clauses.append(f'"{col_name}" AS {clean_name}')
                else:
                    select_clauses.append(f'"{col_name}"')

            # Extract filename for prefix
            select_clauses.append("regexp_extract(filename, '([^/]+)_\\d{4}-\\d{2}-\\d{2}T', 1) AS prefix")

            # Create clean final table
            select_str = ", ".join(select_clauses)
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_events AS
                SELECT {select_str}
                FROM raw_events_temp
            """)

            # Drop temp table
            self.conn.execute("DROP TABLE raw_events_temp")

            row_count = self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
            logger.info(f"Final table created with {row_count:,} rows from {len(successful_paths)} files")
        else:
            # Create empty table if no successful files
            self.conn.execute("CREATE OR REPLACE TABLE raw_events AS SELECT NULL AS dummy WHERE FALSE")
            logger.warning("No successful files to load. Created empty raw_events table")

        if failed_files:
            logger.warning(f"{len(failed_files)} files failed and were moved to DLQ")

        return self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]

    def _move_file_to_dlq(self, s3_path: str, error_reason: str) -> None:
        """Move a failed file to the DLQ bucket."""
        try:
            # Parse source S3 path
            parts = s3_path.replace("s3://", "").split("/", 1)
            source_bucket = parts[0]
            source_key = parts[1]

            # Get the filename from the key
            filename = source_key.split("/")[-1]

            # Destination DLQ path
            dlq_bucket = "dev-raw-data-testing"
            dlq_prefix = "dev-iceberg-dlq"
            dlq_key = f"{dlq_prefix}/{filename}"

            s3_client = boto3.client('s3', region_name='eu-west-1')

            # Copy file to DLQ
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=dlq_bucket, Key=dlq_key)

            logger.info(f"Moved failed file to DLQ: s3://{dlq_bucket}/{dlq_key}")
            logger.debug(f"Error reason: {error_reason}")

            # Optional: Create a metadata file with error details
            metadata_key = f"{dlq_prefix}/{filename}.error.txt"
            s3_client.put_object(
                Bucket=dlq_bucket,
                Key=metadata_key,
                Body=f"Error: {error_reason}\nOriginal file: {s3_path}\nTimestamp: {datetime.now(timezone.utc).isoformat()}"
            )
            logger.debug(f"Created error metadata file: s3://{dlq_bucket}/{metadata_key}")

        except Exception as e:
            logger.error(f"Failed to move file to DLQ: {e}")
            raise

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

    def run(self) -> JobMetrics:
        """Execute the complete ETL pipeline."""
        logger.info("Starting S3 to Iceberg ETL Pipeline")
        logger.info(f"Processing files from S3: s3://{self.config.s3_source_bucket}/{self.config.s3_source_prefix}")

        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Phase 1: Discover CSV.gz files from S3
            s3_paths = self._run_phase1_discover_files()
            if not s3_paths:
                logger.warning("No source files found. Exiting.")
                return self.metrics

            # Phase 2: Load data into DuckDB
            processor = self._run_phase2_load_data(s3_paths)

            # Phase 3: Process/transform data
            self._run_phase3_process_data(processor, current_time)

            # Phase 4: Write partitioned tables
            self._run_phase4_write_partitioned(processor)

            # Phase 5: Write aggregates
            self._run_phase5_write_aggregates(processor, current_time)

            processor.close()

        except Exception as e:
            logger.exception(f"Pipeline failed with error: {e}")
            raise

        self.metrics.log_summary()
        return self.metrics

    def _run_phase1_discover_files(self) -> list[str]:
        """Phase 1: Discover CSV.gz files from S3 source bucket."""
        logger.info("PHASE 1: Discovering CSV.gz files from S3")
        start = time.perf_counter()

        discoverer = S3FileDiscovery(self.config)
        s3_paths = discoverer.discover_files()
        discoverer.close()

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 1 - File Discovery"] = duration
        logger.info(f"PHASE 1 completed: {len(s3_paths)} files discovered in {duration:.2f}s")

        return s3_paths

    def _run_phase2_load_data(self, s3_paths: list[str]) -> DuckDBProcessor:
        """Phase 2: Load data into DuckDB."""
        logger.info("PHASE 2: Loading data into DuckDB")
        start = time.perf_counter()

        processor = DuckDBProcessor(self.config)
        self.metrics.rows_read = processor.load_csv_files(s3_paths)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 2 - Data Loading"] = duration
        logger.info(f"PHASE 2 completed: {self.metrics.rows_read:,} rows in {duration:.2f}s")

        return processor

    def _run_phase3_process_data(self, processor: DuckDBProcessor, current_time: str) -> None:
        """Phase 3: Process and transform data."""
        logger.info("PHASE 3: Processing data")
        start = time.perf_counter()

        processor.process_dataframe(current_time)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 3 - Data Processing"] = duration
        logger.info(f"PHASE 3 completed in {duration:.2f}s")

    def _run_phase4_write_partitioned(self, processor: DuckDBProcessor) -> None:
        """Phase 4: Write partitioned tables."""
        logger.info("PHASE 4: Writing partitioned tables")
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
        self.metrics.phase_timings["Phase 4 - Partitioned Tables"] = duration
        logger.info(
            f"PHASE 4 completed: {self.metrics.partitioned_rows_written:,} rows in {duration:.2f}s"
        )

    def _run_phase5_write_aggregates(
        self, processor: DuckDBProcessor, current_time: str
    ) -> None:
        """Phase 5: Write aggregate tables."""
        logger.info("PHASE 5: Writing aggregate tables")
        start = time.perf_counter()

        rows, failed = processor.write_ad_impression_aggregates(current_time)
        self.metrics.aggregate_rows_written = rows
        self.metrics.failed_tables.extend(failed)

        duration = time.perf_counter() - start
        self.metrics.phase_timings["Phase 5 - Aggregates"] = duration
        logger.info(f"PHASE 5 completed: {rows:,} rows in {duration:.2f}s")


# ============================================================================
# CLI Application
# ============================================================================

app = typer.Typer(
    name="s3-to-iceberg-etl",
    help="ETL Pipeline: S3 → Iceberg (DuckDB)",
    add_completion=False,
)


@app.command()
def run(
    s3_source_bucket: Annotated[
        str,
        typer.Option(
            "--s3-source-bucket",
            help="S3 source bucket name containing CSV.gz files",
        ),
    ] = S3_SOURCE_BUCKET,
    s3_source_prefix: Annotated[
        str,
        typer.Option(
            "--s3-source-prefix",
            help="S3 prefix path for source CSV.gz files",
        ),
    ] = S3_SOURCE_PREFIX,
    s3_bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="S3 destination bucket name for output",
        ),
    ] = S3_BUCKET_NAME,
    s3_output_path: Annotated[
        str,
        typer.Option(
            "--s3-output-path",
            help="S3 path for output parquet files",
        ),
    ] = S3_OUTPUT_PATH,
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
) -> None:
    """
    Run the S3 to Iceberg ETL pipeline.

    This pipeline discovers CSV.gz files in S3, transforms them using DuckDB,
    and writes the results as partitioned Parquet files.
    """
    configure_logging(verbose=verbose, log_file=log_file)

    config = ETLConfig(
        s3_source_bucket=s3_source_bucket,
        s3_source_prefix=s3_source_prefix,
        s3_bucket=s3_bucket,
        s3_output_path=s3_output_path,
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
def validate_config(
    s3_source_bucket: Annotated[
        str,
        typer.Option(
            "--s3-source-bucket",
            help="S3 source bucket name",
        ),
    ] = S3_SOURCE_BUCKET,
    s3_bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="S3 destination bucket name",
        ),
    ] = S3_BUCKET_NAME,
) -> None:
    """Validate the configuration and check connectivity."""
    configure_logging(verbose=True)

    logger.info("Validating configuration...")

    # Check source S3 bucket connectivity
    try:
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=s3_source_bucket)
        logger.info(f"✓ S3 source bucket '{s3_source_bucket}' is accessible")
    except Exception as e:
        logger.error(f"✗ S3 source bucket check failed: {e}")

    # Check destination S3 bucket connectivity
    try:
        s3 = boto3.client("s3")
        s3.head_bucket(Bucket=s3_bucket)
        logger.info(f"✓ S3 destination bucket '{s3_bucket}' is accessible")
    except Exception as e:
        logger.error(f"✗ S3 destination bucket check failed: {e}")

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
